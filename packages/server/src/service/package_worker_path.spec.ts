/**
 * Integration test: exercise `Package.create` with the package-load
 * worker pool enabled (PACKAGE_LOAD_WORKERS=1).
 *
 * Validates that the worker-load path:
 *   - reads the manifest, probes embedded databases, and compiles
 *     every model in a single off-thread job
 *   - produces a live `Package` whose `Model`s have populated
 *     `modelDef` / `sources` / `queries`
 *   - hydrates the `ModelMaterializer` from `modelDef` on first
 *     query (no recompile) — verified end-to-end by running a
 *     query through the resulting Model and getting a result
 *
 * Kept separate from `package.spec.ts` so the existing tests keep
 * running on the in-process path without paying worker startup cost.
 *
 * Pool reuse strategy: one `PackageLoadPool` shared across all
 * cases in this file. Spawning a fresh worker per test crashes Bun
 * (segfault) because DuckDB's native bindings don't tolerate being
 * loaded concurrently into multiple worker isolates of the same Bun
 * process. Production uses one pool; this matches.
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

describe("Package.create via worker pool", () => {
   let tempDir: string;
   let pool: PackageLoadPool;

   beforeAll(async () => {
      process.env.PACKAGE_LOAD_WORKERS = "1";
      pool = new PackageLoadPool(1);
      // Wire our pool into the module-level singleton so Package.create
      // picks it up via getPackageLoadPool().
      await __setPackageLoadPoolForTests(pool);
   });

   afterAll(async () => {
      await __setPackageLoadPoolForTests(null);
      if (ORIGINAL_ENV === undefined) {
         delete process.env.PACKAGE_LOAD_WORKERS;
      } else {
         process.env.PACKAGE_LOAD_WORKERS = ORIGINAL_ENV;
      }
   });

   beforeEach(() => {
      tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "publisher-pkg-worker-"));
   });

   afterEach(() => {
      if (tempDir) {
         // tempDir gets wiped by Package.create on failure (it's the
         // staging-cleanup path); ignore ENOENT here.
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

   function writeManifest(): void {
      fs.writeFileSync(
         path.join(tempDir, "publisher.json"),
         JSON.stringify({ name: "pkg", description: "test package" }),
      );
   }

   it("loads a package end-to-end and serves a query through the hydrated materializer", async () => {
      writeManifest();
      // Define `total_v` as a *view* on the source so the query
      // builder's `run: nums -> total_v` form resolves. (Top-level
      // queries take the `run: total_q` form — orthogonal path that
      // the in-process tests already cover.)
      fs.writeFileSync(
         path.join(tempDir, "trivial.malloy"),
         `source: nums is duckdb.sql("select 1 as a, 2 as b") extend {
  measure: total is a.sum()
  view: total_v is { aggregate: total }
}`,
      );

      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         const pkg = await Package.create("env", "pkg", tempDir, malloyConfig);
         expect(pkg).toBeInstanceOf(Package);
         expect(pkg.getModelPaths()).toEqual(["trivial.malloy"]);

         const model = pkg.getModel("trivial.malloy");
         expect(model).toBeDefined();
         const apiModel = (await model!.getModel()) as {
            modelDef?: string;
            sources?: { name?: string }[];
         };
         expect(apiModel.modelDef).toBeDefined();
         expect(apiModel.sources?.[0]?.name).toBe("nums");

         // First query against the package — hydrates the
         // ModelMaterializer from the worker's modelDef without a
         // recompile, then runs the SQL against the *main thread's*
         // DuckDB connection (the only one with the in-memory `nums`
         // source loaded via duckdb.sql()).
         const { result } = await model!.getQueryResults(
            "nums",
            "total_v",
            undefined,
         );
         expect(result.data).toBeDefined();
      } finally {
         await duckdb.close();
      }
   });

   it("propagates a per-model compile failure as a thrown error from Package.create", async () => {
      writeManifest();
      fs.writeFileSync(
         path.join(tempDir, "broken.malloy"),
         `source: bad is duckdb.sql("select 1 as a") extend {
  measure: oops is THIS_FUNC_DOES_NOT_EXIST(a)
}`,
      );

      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         await expect(
            Package.create("env", "pkg", tempDir, malloyConfig),
         ).rejects.toBeInstanceOf(Error);
      } finally {
         await duckdb.close();
      }
   });

   it("validates and surfaces a valid #(authorize) model through the worker", async () => {
      writeManifest();
      fs.writeFileSync(
         path.join(tempDir, "gated.malloy"),
         `##! experimental.givens

given:
  ROLE :: string

#(authorize) "$ROLE = 'analyst'"
source: gated is duckdb.sql("select 1 as id")`,
      );

      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         const pkg = await Package.create("env", "pkg", tempDir, malloyConfig);
         const model = pkg.getModel("gated.malloy");
         const apiModel = (await model!.getModel()) as {
            sources?: { name?: string; authorize?: string[] }[];
         };
         // The worker compiled the authorize probe (no throw) and surfaced the
         // effective expression list — proves worker-path validation runs.
         expect(apiModel.sources?.[0]?.authorize).toEqual([
            "$ROLE = 'analyst'",
         ]);
         expect(model!.getAuthorize("gated")).toEqual(["$ROLE = 'analyst'"]);
      } finally {
         await duckdb.close();
      }
   });

   it("rejects a package whose #(authorize) references an unknown given (worker validation)", async () => {
      writeManifest();
      fs.writeFileSync(
         path.join(tempDir, "badgate.malloy"),
         `##! experimental.givens

given:
  ROLE :: string

#(authorize) "$NOPE = 'x'"
source: gated is duckdb.sql("select 1 as id")`,
      );

      const { ModelCompilationError } = await import("../errors");
      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         // Must surface as a 424 ModelCompilationError across the worker
         // boundary, not a generic 500 — the worker serializes it with
         // isCompilationError so the main thread re-wraps it.
         await expect(
            Package.create("env", "pkg", tempDir, malloyConfig),
         ).rejects.toBeInstanceOf(ModelCompilationError);
      } finally {
         await duckdb.close();
      }
   });

   it("validates a valid #(authorize) source in a .malloynb notebook through the worker", async () => {
      writeManifest();
      fs.writeFileSync(
         path.join(tempDir, "gated.malloynb"),
         `>>>markdown
# Gated notebook

>>>malloy
##! experimental.givens

given:
  ROLE :: string

#(authorize) "$ROLE = 'analyst'"
source: gated is duckdb.sql("select 1 as id")`,
      );

      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         const pkg = await Package.create("env", "pkg", tempDir, malloyConfig);
         const model = pkg.getModel("gated.malloynb");
         // compileNotebookModel ran authorize validation (no throw) and
         // surfaced the gate — the notebook compile path was previously
         // unexercised by tests.
         expect(model!.getAuthorize("gated")).toEqual(["$ROLE = 'analyst'"]);
      } finally {
         await duckdb.close();
      }
   });

   it("rejects a .malloynb notebook whose #(authorize) references an unknown given", async () => {
      writeManifest();
      fs.writeFileSync(
         path.join(tempDir, "badgate.malloynb"),
         `>>>malloy
##! experimental.givens

given:
  ROLE :: string

#(authorize) "$NOPE = 'x'"
source: gated is duckdb.sql("select 1 as id")`,
      );

      const { ModelCompilationError } = await import("../errors");
      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         await expect(
            Package.create("env", "pkg", tempDir, malloyConfig),
         ).rejects.toBeInstanceOf(ModelCompilationError);
      } finally {
         await duckdb.close();
      }
   });

   // NB: kept last in this describe — swapping the singleton for a
   // pre-shutdown pool also tears down the shared `pool` (the swap
   // implementation shuts down the outgoing singleton). Subsequent
   // tests in this describe would see a dead pool. afterAll only
   // resets the singleton to null, so this is safe at the tail.
   it("rewraps pool-infrastructure failures as ServiceUnavailableError (HTTP 503)", async () => {
      writeManifest();
      fs.writeFileSync(
         path.join(tempDir, "trivial.malloy"),
         `source: nums is duckdb.sql("select 1 as a")`,
      );

      const deadPool = new PackageLoadPool(1);
      await deadPool.shutdown();
      await __setPackageLoadPoolForTests(deadPool);

      const { ServiceUnavailableError } = await import("../errors");
      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         await expect(
            Package.create("env", "pkg", tempDir, malloyConfig),
         ).rejects.toBeInstanceOf(ServiceUnavailableError);
      } finally {
         await duckdb.close();
         // Replace the singleton with a fresh, live pool immediately. The
         // outer afterAll also resets to null, but restoring here ensures the
         // shut-down pool never outlives this test -- otherwise, under serial
         // execution, a later spec file that lazily reuses the singleton via
         // getPackageLoadPool() could observe the dead pool before afterAll
         // runs and fail with "PackageLoadPool is shutting down".
         pool = new PackageLoadPool(1);
         await __setPackageLoadPoolForTests(pool);
      }
   });
});
