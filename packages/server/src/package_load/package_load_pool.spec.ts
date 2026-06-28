/**
 * Unit + light integration tests for the PackageLoadPool.
 *
 * The pool runs real `worker_threads` workers under the hood. These
 * tests intentionally exercise that path so we catch regressions in
 * RPC routing, queueing, lifecycle, and error propagation that
 * wouldn't surface in a pure mock.
 *
 * Pool reuse strategy
 * -------------------
 * The "real worker" tests share a single `PackageLoadPool` across
 * cases via Bun's `beforeAll`/`afterAll`. The worker itself owns no
 * native handles (all duckdb work runs on the main thread); we still
 * share to keep per-test overhead low and to match the production
 * deployment, where the pool spawns workers up-front and reuses them.
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
   getPackageLoadWorkerCount,
} from "./package_load_pool";

// ──────────────────────────────────────────────────────────────────────
// getPackageLoadWorkerCount — env var parsing
// ──────────────────────────────────────────────────────────────────────

describe("getPackageLoadWorkerCount", () => {
   const ORIGINAL = process.env.PACKAGE_LOAD_WORKERS;

   afterEach(() => {
      if (ORIGINAL === undefined) {
         delete process.env.PACKAGE_LOAD_WORKERS;
      } else {
         process.env.PACKAGE_LOAD_WORKERS = ORIGINAL;
      }
   });

   it("defaults to 1 worker when env unset", () => {
      delete process.env.PACKAGE_LOAD_WORKERS;
      expect(getPackageLoadWorkerCount()).toBe(1);
   });

   it("throws when env value is non-numeric", () => {
      process.env.PACKAGE_LOAD_WORKERS = "not-a-number";
      expect(() => getPackageLoadWorkerCount()).toThrow(/positive integer/);
   });

   it("throws when env value is negative", () => {
      process.env.PACKAGE_LOAD_WORKERS = "-2";
      expect(() => getPackageLoadWorkerCount()).toThrow(/positive integer/);
   });

   it("throws when env value is 0 (no in-process fallback)", () => {
      process.env.PACKAGE_LOAD_WORKERS = "0";
      expect(() => getPackageLoadWorkerCount()).toThrow(/positive integer/);
   });

   it("honors positive overrides", () => {
      process.env.PACKAGE_LOAD_WORKERS = "4";
      expect(getPackageLoadWorkerCount()).toBe(4);
   });
});

// ──────────────────────────────────────────────────────────────────────
// PackageLoadPool constructor validation
// ──────────────────────────────────────────────────────────────────────

describe("PackageLoadPool constructor", () => {
   it("throws when maxWorkers is 0", () => {
      expect(() => new PackageLoadPool(0)).toThrow(/maxWorkers >= 1/);
   });

   it("throws when maxWorkers is negative", () => {
      expect(() => new PackageLoadPool(-1)).toThrow(/maxWorkers >= 1/);
   });
});

// ──────────────────────────────────────────────────────────────────────
// PackageLoadPool — real worker loads a package
//
// One shared pool across the describe; each test gets its own temp
// package directory and its own DuckDB connection.
// ──────────────────────────────────────────────────────────────────────

describe("PackageLoadPool (real worker)", () => {
   let pool: PackageLoadPool;
   let tempDir: string;

   beforeAll(() => {
      pool = new PackageLoadPool(1);
   });

   afterAll(async () => {
      await pool.shutdown();
      await __setPackageLoadPoolForTests(null);
   });

   beforeEach(() => {
      tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "publisher-compile-"));
   });

   afterEach(() => {
      if (tempDir) {
         fs.rmSync(tempDir, { recursive: true, force: true });
         tempDir = "";
      }
   });

   function writeManifest(dir: string, name = "pkg"): void {
      fs.writeFileSync(
         path.join(dir, "publisher.json"),
         JSON.stringify({ name, description: "test package" }),
      );
   }

   async function buildConfig(): Promise<{
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

   it("loads a trivial single-model package and returns a hydratable modelDef", async () => {
      writeManifest(tempDir);
      fs.writeFileSync(
         path.join(tempDir, "trivial.malloy"),
         `source: nums is duckdb.sql("select 1 as a, 2 as b") extend {
  measure: total is a.sum()
}`,
      );

      const { malloyConfig, duckdb } = await buildConfig();
      try {
         const outcome = await pool.loadPackage({
            packagePath: tempDir,
            packageName: "pkg",
            malloyConfig,
            defaultConnectionName: "duckdb",
         });

         expect(outcome.packageMetadata.name).toBe("pkg");
         expect(outcome.models).toHaveLength(1);
         const m = outcome.models[0];
         expect(m.modelPath).toBe("trivial.malloy");
         expect(m.modelType).toBe("model");
         expect(m.compilationError).toBeUndefined();
         expect(m.modelDef).toBeDefined();
         expect(Array.isArray(m.sources)).toBe(true);
         expect(outcome.loadDurationMs).toBeGreaterThan(0);
      } finally {
         await duckdb.close();
      }
   });

   it("carries the package materialization config (schedule + freshness) across the worker boundary", async () => {
      fs.writeFileSync(
         path.join(tempDir, "publisher.json"),
         JSON.stringify({
            name: "pkg",
            materialization: {
               schedule: "0 6 * * *",
               freshness: { window: "24h", fallback: "stale_ok" },
            },
         }),
      );
      fs.writeFileSync(
         path.join(tempDir, "trivial.malloy"),
         `source: nums is duckdb.sql("select 1 as a")`,
      );

      const { malloyConfig, duckdb } = await buildConfig();
      try {
         const outcome = await pool.loadPackage({
            packagePath: tempDir,
            packageName: "pkg",
            malloyConfig,
            defaultConnectionName: "duckdb",
         });
         expect(outcome.packageMetadata.materialization).toEqual({
            schedule: "0 6 * * *",
            freshness: { window: "24h", fallback: "stale_ok" },
         });
      } finally {
         await duckdb.close();
      }
   });

   it("propagates a per-model compile failure in-band (not as a rejected Promise)", async () => {
      writeManifest(tempDir);
      fs.writeFileSync(
         path.join(tempDir, "broken.malloy"),
         `source: bad is duckdb.sql("select 1 as a") extend {
  measure: oops is THIS_FUNC_DOES_NOT_EXIST(a)
}`,
      );

      const { malloyConfig, duckdb } = await buildConfig();
      try {
         const outcome = await pool.loadPackage({
            packagePath: tempDir,
            packageName: "pkg",
            malloyConfig,
            defaultConnectionName: "duckdb",
         });
         expect(outcome.models).toHaveLength(1);
         const m = outcome.models[0];
         expect(m.compilationError).toBeDefined();
         expect(m.modelDef).toBeUndefined();
      } finally {
         await duckdb.close();
      }
   });

   it("ignores embedded csv/parquet files (database probing is a main-thread concern)", async () => {
      // The worker is pure-CPU: it reads the manifest and compiles
      // .malloy files only. Database probing stays on the main thread
      // (Package.readDatabases) so the worker doesn't need to dlopen
      // duckdb-native. Verify that dropping a .csv next to the model
      // doesn't crash the worker and doesn't show up in the result.
      writeManifest(tempDir);
      fs.writeFileSync(path.join(tempDir, "rows.csv"), "a,b\n1,2\n3,4\n5,6\n");
      fs.writeFileSync(
         path.join(tempDir, "trivial.malloy"),
         `source: nums is duckdb.sql("select 1 as a") extend {\n  measure: total is a.sum()\n}`,
      );

      const { malloyConfig, duckdb } = await buildConfig();
      try {
         const outcome = await pool.loadPackage({
            packagePath: tempDir,
            packageName: "pkg",
            malloyConfig,
            defaultConnectionName: "duckdb",
         });
         expect(outcome.models).toHaveLength(1);
         expect(outcome.models[0].compilationError).toBeUndefined();
      } finally {
         await duckdb.close();
      }
   });
});

// ──────────────────────────────────────────────────────────────────────
// PackageLoadPool — shutdown rejects new submissions
// Separate describe so the shutdown doesn't poison the shared pool above.
// ──────────────────────────────────────────────────────────────────────

describe("PackageLoadPool (shutdown)", () => {
   it("rejects loadPackage() after shutdown()", async () => {
      const { MalloyConfig } = await import("@malloydata/malloy");
      const pool = new PackageLoadPool(1);
      await pool.shutdown();
      await expect(
         pool.loadPackage({
            packagePath: "/tmp/nowhere",
            packageName: "nowhere",
            malloyConfig: new MalloyConfig({ connections: {} }),
            defaultConnectionName: "duckdb",
         }),
      ).rejects.toThrow("shutting down");
   });
});
