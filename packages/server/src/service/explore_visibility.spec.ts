// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

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

   // A base module (never an explore) plus a curated surface that imports it,
   // joins through it, and re-exports only `customers`. `helper` is a second
   // local source the surface does NOT export — used to prove within-file
   // curation.
   //
   // The curated file's NAME is a parameter because "index.malloy" is no longer
   // an arbitrary choice: a root file with that name IS the discovery surface
   // when the manifest declares no `explores`. A test whose subject is the
   // uncurated default has to name the file something else, or it is testing
   // the convention instead.
   function writeLayeredModels(curatedFile = "index.malloy"): void {
      fs.writeFileSync(
         path.join(tempDir, "base.malloy"),
         `source: base_source is duckdb.sql("select 1 as id, 'x' as label")`,
      );
      fs.writeFileSync(
         path.join(tempDir, curatedFile),
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
      // but it carries its own #(access_filter) gate. Curation must not drop that
      // gate: getAuthorize reads the COMPLETE source list, not the curated view.
      writeManifest({ explores: ["index.malloy"] });
      fs.writeFileSync(
         path.join(tempDir, "base.malloy"),
         `##! experimental.givens

given:
  ID :: number

#(access_filter) id = $ID
source: base_source is duckdb.sql("select 1 as id") extend {}`,
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
         expect(model.getAccessFilter("base_source")).toEqual(["id = $ID"]);
      } finally {
         await duckdb.close();
      }
   });

   it("lists every model and full sources when nothing curates (backward compatible)", async () => {
      writeManifest();
      // Deliberately NOT index.malloy: a package is uncurated only when it has
      // neither an `explores` key nor a root index.malloy.
      writeLayeredModels("surface.malloy");

      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         const pkg = await Package.create("env", "pkg", tempDir, malloyConfig);
         expect(await listedModelPaths(pkg)).toEqual([
            "base.malloy",
            "surface.malloy",
         ]);

         // Nothing curates ⇒ export{} curation off; non-exported `helper` listed.
         const apiModel = (await pkg
            .getModel("surface.malloy")!
            .getModel()) as {
            sources?: { name?: string }[];
         };
         const names = (apiModel.sources ?? []).map((s) => s.name).sort();
         expect(names).toContain("customers");
         expect(names).toContain("helper");
      } finally {
         await duckdb.close();
      }
   });

   it("defaults the surface to a root index.malloy when explores is absent", async () => {
      writeManifest(); // no explores at all
      writeLayeredModels(); // writes index.malloy

      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         const pkg = await Package.create("env", "pkg", tempDir, malloyConfig);

         // Same listing the explicit `explores: ["index.malloy"]` produces in
         // the first test of this file, from no manifest key at all.
         expect(await listedModelPaths(pkg)).toEqual(["index.malloy"]);

         // And within-file curation is on, so `helper` is dropped exactly as
         // it is under a declared surface.
         const apiModel = (await pkg.getModel("index.malloy")!.getModel()) as {
            sources?: { name?: string }[];
         };
         expect((apiModel.sources ?? []).map((s) => s.name).sort()).toEqual([
            "customers",
         ]);

         // The derived entry always resolves, so it can never be an invalid
         // explores entry -- getInvalidExplores has nothing to report.
         expect(pkg.getInvalidExplores()).toEqual([]);
      } finally {
         await duckdb.close();
      }
   });

   it("prefers an explicit explores over the convention, and says so", async () => {
      writeManifest({ explores: ["base.malloy"] });
      writeLayeredModels(); // index.malloy exists but is not declared

      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         const pkg = await Package.create("env", "pkg", tempDir, malloyConfig);
         expect(await listedModelPaths(pkg)).toEqual(["base.malloy"]);

         // The author wrote a file the convention would have used and a key
         // that leaves it out. The key wins; nothing else would tell them.
         const warnings = pkg.getPackageMetadata().warnings ?? [];
         // Pinned as the author sees it, on the package they fetch.
         expect(warnings.map((w) => w.message)).toContain(
            `index.malloy is ignored because "explores" in publisher.json ` +
               `doesn't list it. Fix: delete "explores" to publish what ` +
               `index.malloy exports, or rename index.malloy if it isn't meant ` +
               `to decide what is published.`,
         );
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

   it("the model GET shows only what the surface publishes", async () => {
      // index.malloy imports base.malloy whole, so its compiled model carries
      // `hidden` too. The GET must not name it, in any field.
      writeManifest({});
      fs.writeFileSync(
         path.join(tempDir, "base.malloy"),
         `source: pub is duckdb.sql("select 1 as id")
source: hidden is duckdb.sql("select 2 as id")`,
      );
      fs.writeFileSync(
         path.join(tempDir, "index.malloy"),
         // A top-level run over the hidden source puts it in queryList too.
         `import "base.malloy"\nrun: hidden -> { group_by: id }\nexport { pub }`,
      );
      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         const pkg = await Package.create("env", "pkg", tempDir, malloyConfig);
         const index = pkg.getModel("index.malloy")!;
         const response = (await index.getModel()) as {
            modelDef?: string;
            modelInfo?: string;
            sources?: { name?: string }[];
         };
         const modelDef = JSON.parse(response.modelDef ?? "{}") as {
            contents: Record<string, unknown>;
            exports: string[];
            imports?: unknown[];
         };
         expect(Object.keys(modelDef.contents)).toEqual(["pub"]);
         expect(modelDef.exports).toEqual(["pub"]);
         // The app reads `imports`; pruning `contents` must leave it.
         expect(modelDef.imports?.length).toBe(1);
         // Searched as text, not for a quoted name: inside the JSON-encoded
         // modelDef every quote is escaped, so a quoted search never matches.
         // Covers contents, sourceRegistry (keyed name@file) and queryList.
         expect(response.modelDef).not.toContain("hidden");
         expect(response.modelInfo).not.toContain("hidden");
         expect(JSON.stringify(response.sources)).not.toContain("hidden");
         expect(index.showsFileText()).toBe(true);

         // The hidden file is refused outright, with the query route's words.
         expect(() =>
            pkg.getModel("base.malloy")!.assertFileOnSurface(),
         ).toThrow('No queryable model "base.malloy".');

         // A surface file that declares an unexported helper keeps its
         // compiled view but not its text, which would show the helper.
         fs.writeFileSync(
            path.join(tempDir, "index.malloy"),
            `import "base.malloy"
source: helper is duckdb.sql("select 3 as id")
export { pub }`,
         );
         const withHelper = await Package.create(
            "env",
            "pkg",
            tempDir,
            malloyConfig,
         );
         expect(withHelper.getModel("index.malloy")!.showsFileText()).toBe(
            false,
         );

         // With no surface, nothing is curated.
         writeManifest({ explores: [] });
         const open = await Package.create("env", "pkg", tempDir, malloyConfig);
         const openDef = JSON.parse(
            (
               (await open.getModel("index.malloy")!.getModel()) as {
                  modelDef: string;
               }
            ).modelDef,
         ) as { contents: Record<string, unknown> };
         expect(Object.keys(openDef.contents).sort()).toEqual([
            "helper",
            "hidden",
            "pub",
         ]);
         expect(() =>
            open.getModel("base.malloy")!.assertFileOnSurface(),
         ).not.toThrow();
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
         expect(warnings[0].model).toBe("consumer.malloy");
         // One of several possible listed files, so it takes only itself off
         // the surface, and the key it is listed in is still a remedy.
         expect(warnings[0].message).toBe(
            `consumer.malloy exports nothing, so nothing can be queried ` +
               `through it. Fix: add an export { ... } naming the sources to ` +
               `publish, or remove it from "explores".`,
         );
         // Advisory warnings also ride the package metadata (the QA gap:
         // exploresWarnings said none while a listed file surfaced nothing).
         expect(
            (pkg.getPackageMetadata().warnings ?? []).some(
               (w) => w.model === "consumer.malloy",
            ),
         ).toBe(true);
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

   it("explains itself when a broken index.malloy takes the whole package down", async () => {
      // `reloadAllModels` DIRECTLY, which is the narrow path this warning is
      // for: it installs a placeholder for the file that failed and does not
      // go through Environment.loadPackage, so the package keeps serving with
      // an empty surface and is never marked stale. Verified against a live
      // server: first load fails the package outright (loadErrors), and every
      // author-facing reload -- watcher, MCP reload_package, REST ?reload=true
      // -- goes through loadPackage, which keeps the last good model and
      // reports `stale: true` with the compile error. Only the materialization
      // / manifest rebind paths land here.
      writeManifest({});
      fs.writeFileSync(
         path.join(tempDir, "orders.malloy"),
         `source: orders is duckdb.sql("select 1 as id")\nexport { orders }`,
      );
      fs.writeFileSync(
         path.join(tempDir, "index.malloy"),
         `import "orders.malloy"\nexport { orders }`,
      );

      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         const pkg = await Package.create("env", "pkg", tempDir, malloyConfig);
         expect(pkg.brokenSurfaceWarnings()).toEqual([]);

         // Now break it the way an author does: a typo in the export list.
         fs.writeFileSync(
            path.join(tempDir, "index.malloy"),
            `import "orders.malloy"\nexport { ordrs }`,
         );
         await pkg.reloadAllModels({});

         const warnings = pkg.brokenSurfaceWarnings();
         expect(warnings.length).toBe(1);
         expect(warnings[0].model).toBe("index.malloy");
         // The point of the message: orders.malloy compiled fine and is still
         // refused, and nothing else in the system says why.
         expect(warnings[0].message).toContain(
            "is this package's whole discovery surface and failed to compile",
         );
         expect(warnings[0].message).toContain("including the 1 that compiled");
         // One message for the surface, not one per file on it.
         expect(warnings.length).toBe(1);
         expect(warnings[0].message).toContain("404");

         // It must ride the API, not just the log: the operator sees the 404
         // on orders.malloy, not the compile error on index.malloy.
         expect(
            (pkg.getPackageMetadata().warnings ?? []).some((w) =>
               (w.message ?? "").includes("whole discovery surface"),
            ),
         ).toBe(true);

         // Deliberately still fail-CLOSED: a typo must not expose what the
         // author curated away.
         expect(await listedModelPaths(pkg)).toEqual(["index.malloy"]);

         // And it clears on the save that compiles.
         fs.writeFileSync(
            path.join(tempDir, "index.malloy"),
            `import "orders.malloy"\nexport { orders }`,
         );
         await pkg.reloadAllModels({});
         expect(pkg.brokenSurfaceWarnings()).toEqual([]);
      } finally {
         await duckdb.close();
      }
   });

   it("reports a lost surface on an in-place reload, and only once", async () => {
      // The materialization / manifest rebind paths reload the SAME Package
      // rather than swapping in a new one, so the notice has to be raised here
      // or it is never raised: a later full reload compares against a surface
      // this reload has already cleared.
      writeManifest({});
      writeLayeredModels();

      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         const pkg = await Package.create("env", "pkg", tempDir, malloyConfig);
         const widened = () =>
            (pkg.getPackageMetadata().warnings ?? []).find((w) =>
               (w.message ?? "").includes("publishes no surface now"),
            );
         await pkg.reloadAllModels({});
         expect(widened()).toBeUndefined();

         fs.unlinkSync(path.join(tempDir, "index.malloy"));
         await pkg.reloadAllModels({});
         expect(pkg.getPackageMetadata().explores).toBeUndefined();
         expect(widened()?.message).toContain(
            'This package published "index.malloy" before the last reload',
         );

         await pkg.reloadAllModels({});
         expect(widened()).toBeUndefined();
      } finally {
         await duckdb.close();
      }
   });

   it("reports a multi-file surface once, and counts only what compiled", async () => {
      // Two broken files on one surface used to produce two messages, each
      // claiming to BE the whole surface, and the collateral count included
      // hidden models that had failed to compile on their own.
      writeManifest({ explores: ["a.malloy", "b.malloy"] });
      for (const name of ["a", "b"]) {
         fs.writeFileSync(
            path.join(tempDir, `${name}.malloy`),
            `source: ${name}_src is duckdb.sql("select 1 as id")\nexport { ${name}_src }`,
         );
      }
      fs.writeFileSync(
         path.join(tempDir, "fine.malloy"),
         `source: fine is duckdb.sql("select 1 as id")\nexport { fine }`,
      );
      fs.writeFileSync(
         path.join(tempDir, "alsobroken.malloy"),
         `source: ab is duckdb.sql("select 1 as id")\nexport { ab }`,
      );

      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         const pkg = await Package.create("env", "pkg", tempDir, malloyConfig);
         for (const name of ["a", "b"]) {
            fs.writeFileSync(
               path.join(tempDir, `${name}.malloy`),
               `source: ${name}_src is duckdb.sql("select 1 as id")\nexport { nope }`,
            );
         }
         // A hidden model broken on its own account: it was not taken down by
         // the surface, so it must not be counted as collateral.
         fs.writeFileSync(
            path.join(tempDir, "alsobroken.malloy"),
            `source: ab is duckdb.sql("select 1 as id")\nexport { nope }`,
         );
         await pkg.reloadAllModels({});

         const warnings = pkg.brokenSurfaceWarnings();
         expect(warnings.length).toBe(1);
         expect(warnings[0].message).toContain(
            "Every model on this package's discovery surface",
         );
         expect(warnings[0].message).not.toContain("whole discovery surface");
         // fine.malloy only: alsobroken.malloy is broken on its own.
         expect(warnings[0].message).toContain("including the 1 that compiled");
         // Both compile errors are named, so the author has both to fix.
         expect(warnings[0].message).toContain("a.malloy:");
         expect(warnings[0].message).toContain("b.malloy:");
      } finally {
         await duckdb.close();
      }
   });

   it("says nothing can be queried when index.malloy is the whole surface and exports nothing", async () => {
      // A hand-written explores of just index.malloy is the same surface as
      // the convention, so it gets the same words.
      writeManifest({ explores: ["index.malloy"] });
      fs.writeFileSync(
         path.join(tempDir, "base.malloy"),
         `source: base_source is duckdb.sql("select 1 as id")`,
      );
      fs.writeFileSync(
         path.join(tempDir, "index.malloy"),
         `import "base.malloy"\nrun: base_source -> { group_by: id }`,
      );

      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         const pkg = await Package.create("env", "pkg", tempDir, malloyConfig);
         const warnings = pkg.emptyDiscoveryWarnings();
         expect(warnings.length).toBe(1);
         expect(warnings[0].message).toBe(
            `index.malloy exports nothing, so nothing in this package can be ` +
               `queried. Fix: add an export { ... } naming the sources to ` +
               `publish.`,
         );
      } finally {
         await duckdb.close();
      }
   });

   it("stays quiet when only PART of the surface is broken", async () => {
      // One broken file beside a working one still leaves a surface, and that
      // file's own compile error is report enough.
      writeManifest({ explores: ["good.malloy", "bad.malloy"] });
      fs.writeFileSync(
         path.join(tempDir, "good.malloy"),
         `source: good is duckdb.sql("select 1 as id")\nexport { good }`,
      );
      fs.writeFileSync(
         path.join(tempDir, "bad.malloy"),
         `source: bad is duckdb.sql("select 1 as id")\nexport { bad }`,
      );

      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         const pkg = await Package.create("env", "pkg", tempDir, malloyConfig);
         fs.writeFileSync(
            path.join(tempDir, "bad.malloy"),
            `source: bad is duckdb.sql("select 1 as id")\nexport { baad }`,
         );
         await pkg.reloadAllModels({});
         expect(pkg.brokenSurfaceWarnings()).toEqual([]);
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
            /report\.malloynb.*notebooks are always listed/s,
         );
      } finally {
         await duckdb.close();
      }
   });
});
