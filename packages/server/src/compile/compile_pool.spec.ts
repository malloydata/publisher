/**
 * Unit + light integration tests for the CompileWorkerPool.
 *
 * The pool runs real `worker_threads` workers under the hood. These
 * tests intentionally exercise that path so we catch regressions in
 * RPC routing, lifecycle, and error propagation that wouldn't surface
 * in a pure mock.
 */
import {
   afterAll,
   afterEach,
   beforeEach,
   describe,
   expect,
   it,
} from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import {
   CompileWorkerPool,
   __setCompilePoolForTests,
   getCompileWorkerCount,
} from "./compile_pool";

// ──────────────────────────────────────────────────────────────────────
// getCompileWorkerCount — env var parsing
// ──────────────────────────────────────────────────────────────────────

describe("getCompileWorkerCount", () => {
   const ORIGINAL = process.env.MALLOY_COMPILE_WORKERS;

   afterEach(() => {
      if (ORIGINAL === undefined) {
         delete process.env.MALLOY_COMPILE_WORKERS;
      } else {
         process.env.MALLOY_COMPILE_WORKERS = ORIGINAL;
      }
   });

   it("defaults to 0 when env unset (ship-dark default)", () => {
      delete process.env.MALLOY_COMPILE_WORKERS;
      expect(getCompileWorkerCount()).toBe(0);
   });

   it("returns 0 when env value is invalid", () => {
      process.env.MALLOY_COMPILE_WORKERS = "not-a-number";
      expect(getCompileWorkerCount()).toBe(0);
   });

   it("returns 0 when env value is negative", () => {
      process.env.MALLOY_COMPILE_WORKERS = "-2";
      expect(getCompileWorkerCount()).toBe(0);
   });

   it("honors positive overrides", () => {
      process.env.MALLOY_COMPILE_WORKERS = "4";
      expect(getCompileWorkerCount()).toBe(4);
   });

   it("treats 0 as a kill switch", () => {
      process.env.MALLOY_COMPILE_WORKERS = "0";
      expect(getCompileWorkerCount()).toBe(0);
   });
});

// ──────────────────────────────────────────────────────────────────────
// CompileWorkerPool — disabled pool
// ──────────────────────────────────────────────────────────────────────

describe("CompileWorkerPool (disabled)", () => {
   it("reports enabled=false when size is 0", () => {
      const pool = new CompileWorkerPool(0);
      expect(pool.enabled).toBe(false);
      expect(pool.size).toBe(0);
   });

   it("throws if compile() is called while disabled", async () => {
      const pool = new CompileWorkerPool(0);
      await expect(
         pool.compile({
            packagePath: "/tmp/nowhere",
            modelPath: "x.malloy",
            malloyConfig: undefined as never,
            defaultConnectionName: "duckdb",
         }),
      ).rejects.toThrow("disabled");
   });

   it("shutdown is a no-op on a disabled pool", async () => {
      const pool = new CompileWorkerPool(0);
      await pool.shutdown();
   });
});

// ──────────────────────────────────────────────────────────────────────
// CompileWorkerPool — real worker compiles a trivial Malloy file
//
// We spin up a real worker and a real (in-memory) DuckDB connection
// the worker proxies back to. The model is intentionally tiny so the
// test runs fast even on cold-start; the goal is to validate the
// wiring (RPC routing, error propagation, modelDef serialization),
// not Malloy's compile semantics.
// ──────────────────────────────────────────────────────────────────────

describe("CompileWorkerPool (real worker)", () => {
   let pool: CompileWorkerPool | null = null;
   let tempDir: string | null = null;

   beforeEach(() => {
      tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "publisher-compile-"));
   });

   afterEach(async () => {
      if (pool) {
         await pool.shutdown();
         pool = null;
      }
      if (tempDir) {
         fs.rmSync(tempDir, { recursive: true, force: true });
         tempDir = null;
      }
      await __setCompilePoolForTests(null);
   });

   afterAll(async () => {
      await __setCompilePoolForTests(null);
   });

   it("compiles a trivial Malloy model and returns a modelDef", async () => {
      // Lazy-import malloy + duckdb so the disabled-pool tests above
      // don't pay this load cost. Inside this it() we know we're
      // committing to an integration-style run.
      const { MalloyConfig, FixedConnectionMap } = await import(
         "@malloydata/malloy"
      );
      const { DuckDBConnection } = await import("@malloydata/db-duckdb");

      if (!tempDir) throw new Error("tempDir not initialized");

      // Trivial Malloy: defines a single source from an in-memory
      // table. No `table('...')` reference means the compiler only
      // does a single `lookupConnection` lookup (for the digest); no
      // schema RPCs round-trip. This keeps the test deterministic
      // and fast.
      const modelSrc = `
source: nums is duckdb.sql("select 1 as a, 2 as b") extend {
  measure: total is a.sum()
}
      `.trim();
      fs.writeFileSync(path.join(tempDir, "trivial.malloy"), modelSrc);

      const duckdb = new DuckDBConnection("duckdb", ":memory:");
      const connections = new FixedConnectionMap(
         new Map([["duckdb", duckdb]]),
         "duckdb",
      );
      const malloyConfig = new MalloyConfig({ connections: {} });
      malloyConfig.wrapConnections(() => connections);

      pool = new CompileWorkerPool(1);
      const outcome = await pool.compile({
         packagePath: tempDir,
         modelPath: "trivial.malloy",
         malloyConfig,
         defaultConnectionName: "duckdb",
      });

      expect(outcome.modelDef).toBeDefined();
      // Sanity-check that sources/queries were extracted, since
      // these go through the same code as the in-process path.
      expect(Array.isArray(outcome.sources)).toBe(true);
      expect(outcome.compileDurationMs).toBeGreaterThan(0);

      await duckdb.close();
   });

   it("propagates compilation errors from the worker as Error instances", async () => {
      const { MalloyConfig, FixedConnectionMap } = await import(
         "@malloydata/malloy"
      );
      const { DuckDBConnection } = await import("@malloydata/db-duckdb");
      if (!tempDir) throw new Error("tempDir not initialized");

      // Syntactically broken Malloy — should produce a MalloyError
      // inside the worker that the pool re-wraps into a
      // ModelCompilationError on the main thread.
      fs.writeFileSync(
         path.join(tempDir, "broken.malloy"),
         "source: bogus is duckdb.sql('not really sql')\n  measure: missing_close_brace is 1",
      );

      const duckdb = new DuckDBConnection("duckdb", ":memory:");
      const connections = new FixedConnectionMap(
         new Map([["duckdb", duckdb]]),
         "duckdb",
      );
      const malloyConfig = new MalloyConfig({ connections: {} });
      malloyConfig.wrapConnections(() => connections);

      pool = new CompileWorkerPool(1);
      await expect(
         pool.compile({
            packagePath: tempDir,
            modelPath: "broken.malloy",
            malloyConfig,
            defaultConnectionName: "duckdb",
         }),
      ).rejects.toBeInstanceOf(Error);

      await duckdb.close();
   });

   it("rejects compile() after shutdown()", async () => {
      const { MalloyConfig } = await import("@malloydata/malloy");
      pool = new CompileWorkerPool(1);
      await pool.shutdown();
      await expect(
         pool.compile({
            packagePath: "/tmp/nowhere",
            modelPath: "x.malloy",
            malloyConfig: new MalloyConfig({ connections: {} }),
            defaultConnectionName: "duckdb",
         }),
      ).rejects.toThrow("shutting down");
   });
});
