/**
 * Integration test: Query Boundary (`queryableSources`).
 *
 * Drives `Package.create` through the package-load worker pool and verifies the
 * rule "queryable == discoverable": under the default `queryableSources:
 * "declared"`, only the discovery surface (`explores` files + their `export {}`
 * closure) is a valid top-level query target. Everything else still compiles,
 * imports, joins, and extends, but is denied a direct query with a generic 404
 * (`NotQueryableError`). `"all"` decouples the axes (the prior behavior), and a
 * package with no `explores` is unaffected in either mode.
 *
 * Enforcement is two-step (mirroring the authorize gate): an early
 * pre-compilation gate that positively denies what it can resolve (schema-
 * oracle defense), and a compiled backstop that settles everything else
 * against the run target Malloy actually executes (read off the prepared
 * query's structRef — inspecting compiler output, never altering compilation).
 * Ad-hoc derivation over a curated source is admitted via the same alias walk
 * filters use; an unresolvable target fails closed.
 *
 * This is the *what* axis; `#(authorize)` (the *who* axis) is orthogonal and
 * tested in explore_visibility.spec.ts / the authorize specs. Notebooks are
 * always public and never gated.
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
import { NotQueryableError } from "../errors";
import { Package } from "./package";

const ORIGINAL_ENV = process.env.PACKAGE_LOAD_WORKERS;

describe("Query Boundary (queryableSources) via worker pool", () => {
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
      tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "publisher-qbound-"));
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

   // base.malloy (a building block, never an explore) + index.malloy (the
   // explore) which imports/joins it, exports only `customers`, and keeps a
   // local `helper` source it does NOT export. Each runnable source carries a
   // view so it can actually be queried.
   function writeLayeredModels(): void {
      fs.writeFileSync(
         path.join(tempDir, "base.malloy"),
         `source: base_source is duckdb.sql("select 1 as id, 5 as n") extend {
  measure: total_n is n.sum()
  view: v is { aggregate: total_n }
}`,
      );
      fs.writeFileSync(
         path.join(tempDir, "index.malloy"),
         `import "base.malloy"
source: helper is duckdb.sql("select 1 as id") extend {
  measure: c is count()
  view: hv is { aggregate: c }
}
source: customers is duckdb.sql("select 1 as id, 100 as amt") extend {
  join_one: b is base_source on id = b.id
  measure: total is amt.sum()
  view: v is { aggregate: total }
}
export { customers }`,
      );
      fs.writeFileSync(
         path.join(tempDir, "report.malloynb"),
         `>>>markdown\n# Report\nAlways public.`,
      );
   }

   // -- default mode ("declared") -----------------------------------------

   it("declared: exported source in an explores file IS queryable (incl. join-through)", async () => {
      writeManifest({ explores: ["index.malloy"] }); // queryableSources defaults to "declared"
      writeLayeredModels();
      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         const pkg = await Package.create("env", "pkg", tempDir, malloyConfig);
         const { result } = await pkg
            .getModel("index.malloy")!
            .getQueryResults("customers", "v", undefined);
         expect(result.data).toBeDefined();
      } finally {
         await duckdb.close();
      }
   });

   it("declared: a non-explores model file is not queryable (file-level, 404)", async () => {
      writeManifest({ explores: ["index.malloy"] });
      writeLayeredModels();
      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         const pkg = await Package.create("env", "pkg", tempDir, malloyConfig);
         // base.malloy is hidden (not an explore) → not a query entry point.
         await expect(
            pkg
               .getModel("base.malloy")!
               .getQueryResults("base_source", "v", undefined),
         ).rejects.toBeInstanceOf(NotQueryableError);
      } finally {
         await duckdb.close();
      }
   });

   it("declared: a non-exported source inside an explores file is not queryable", async () => {
      writeManifest({ explores: ["index.malloy"] });
      writeLayeredModels();
      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         const pkg = await Package.create("env", "pkg", tempDir, malloyConfig);
         // `helper` compiles and is joinable, but is not in index's export{}.
         await expect(
            pkg.getModel("index.malloy")!.getQueryResults("helper", "hv"),
         ).rejects.toBeInstanceOf(NotQueryableError);
      } finally {
         await duckdb.close();
      }
   });

   it("declared: ad-hoc query is admitted for a curated source, denied for a hidden one", async () => {
      writeManifest({ explores: ["index.malloy"] });
      writeLayeredModels();
      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         const pkg = await Package.create("env", "pkg", tempDir, malloyConfig);
         const model = pkg.getModel("index.malloy")!;

         const { result } = await model.getQueryResults(
            undefined,
            undefined,
            "run: customers -> { aggregate: total }",
         );
         expect(result.data).toBeDefined();

         await expect(
            model.getQueryResults(
               undefined,
               undefined,
               "run: helper -> { aggregate: c }",
            ),
         ).rejects.toBeInstanceOf(NotQueryableError);
      } finally {
         await duckdb.close();
      }
   });

   it("declared: multi-statement is settled by the COMPILED run target (last statement wins)", async () => {
      writeManifest({ explores: ["index.malloy"] });
      writeLayeredModels();
      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         const pkg = await Package.create("env", "pkg", tempDir, malloyConfig);
         const model = pkg.getModel("index.malloy")!;

         // Curated decoy first, hidden real target last. The early gate only
         // sees the first `run:` (curated → defers); the compiled backstop
         // resolves the LAST statement — the one Malloy actually executes —
         // and denies it. This is the case the structRef read exists for.
         await expect(
            model.getQueryResults(
               undefined,
               undefined,
               "run: customers -> { aggregate: total }\nrun: helper -> { aggregate: c }",
            ),
         ).rejects.toBeInstanceOf(NotQueryableError);

         // Hidden target FIRST is positively denied by the early gate, before
         // compilation — its compile errors can't be used as a schema oracle.
         await expect(
            model.getQueryResults(
               undefined,
               undefined,
               "run: helper -> { aggregate: c }\nrun: customers -> { aggregate: total }",
            ),
         ).rejects.toBeInstanceOf(NotQueryableError);

         // All-curated multi-statement is legitimate and admitted (no false
         // denial from the old fail-closed-on-shape heuristic).
         const { result } = await model.getQueryResults(
            undefined,
            undefined,
            "run: customers -> { aggregate: total }\nrun: customers -> { group_by: id }",
         );
         expect(result.data).toBeDefined();
      } finally {
         await duckdb.close();
      }
   });

   it("declared: ad-hoc derivation over a curated source is queryable; over a hidden one is not", async () => {
      writeManifest({ explores: ["index.malloy"] });
      writeLayeredModels();
      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         const pkg = await Package.create("env", "pkg", tempDir, malloyConfig);
         const model = pkg.getModel("index.malloy")!;

         // Composing over a queryable source is itself queryable: the compiled
         // structRef names the ad-hoc alias `x`, and the derivation walk maps
         // x → customers (curated).
         const { result } = await model.getQueryResults(
            undefined,
            undefined,
            "source: x is customers extend { measure: m is count() }\nrun: x -> { aggregate: m }",
         );
         expect(result.data).toBeDefined();

         // Laundering a hidden source through an ad-hoc alias is denied: the
         // chain y → helper never reaches the curated surface.
         await expect(
            model.getQueryResults(
               undefined,
               undefined,
               "source: y is helper extend { measure: m is count() }\nrun: y -> { aggregate: m }",
            ),
         ).rejects.toBeInstanceOf(NotQueryableError);
      } finally {
         await duckdb.close();
      }
   });

   it("declared: an exported named query reading a hidden source is admitted by name", async () => {
      // Exporting a query is the author's deliberate exposure of a result,
      // even when the source it reads stays hidden. The explicit queryName
      // request is cleared by the early gate and the compiled backstop is
      // skipped — re-deriving the underlying (hidden) source must not re-deny
      // the author's chosen entry point.
      writeManifest({ explores: ["index.malloy"] });
      fs.writeFileSync(
         path.join(tempDir, "index.malloy"),
         `source: helper is duckdb.sql("select 1 as id") extend {
  measure: c is count()
}
source: customers is duckdb.sql("select 1 as id, 100 as amt") extend {
  measure: total is amt.sum()
}
query: helper_stats is helper -> { aggregate: c }
export { customers, helper_stats }`,
      );
      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         const pkg = await Package.create("env", "pkg", tempDir, malloyConfig);
         const model = pkg.getModel("index.malloy")!;

         const { result } = await model.getQueryResults(
            undefined,
            "helper_stats",
            undefined,
         );
         expect(result.data).toBeDefined();

         // …while the hidden source itself stays non-queryable directly.
         await expect(
            model.getQueryResults(
               undefined,
               undefined,
               "run: helper -> { aggregate: c }",
            ),
         ).rejects.toBeInstanceOf(NotQueryableError);
      } finally {
         await duckdb.close();
      }
   });

   // -- "all" mode (decoupled) --------------------------------------------

   it("all: explores gates discovery only — hidden file/source stay queryable", async () => {
      writeManifest({ explores: ["index.malloy"], queryableSources: "all" });
      writeLayeredModels();
      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         const pkg = await Package.create("env", "pkg", tempDir, malloyConfig);

         // Listing is still curated to the explore.
         const listed = (await pkg.listModels()).map((m) => m.path);
         expect(listed).toEqual(["index.malloy"]);

         // …but the non-explores file and the non-exported source are queryable.
         const base = await pkg
            .getModel("base.malloy")!
            .getQueryResults("base_source", "v", undefined);
         expect(base.result.data).toBeDefined();
         const helper = await pkg
            .getModel("index.malloy")!
            .getQueryResults("helper", "hv", undefined);
         expect(helper.result.data).toBeDefined();
      } finally {
         await duckdb.close();
      }
   });

   // -- no explores declared (boundary inert) -----------------------------

   it("declared default + no explores: everything stays queryable (backward compatible)", async () => {
      writeManifest(); // no explores → no curated surface to enforce
      writeLayeredModels();
      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         const pkg = await Package.create("env", "pkg", tempDir, malloyConfig);
         const base = await pkg
            .getModel("base.malloy")!
            .getQueryResults("base_source", "v", undefined);
         expect(base.result.data).toBeDefined();
         // `helper` is non-exported, but with no explores there is no boundary.
         const helper = await pkg
            .getModel("index.malloy")!
            .getQueryResults("helper", "hv", undefined);
         expect(helper.result.data).toBeDefined();
      } finally {
         await duckdb.close();
      }
   });
});
