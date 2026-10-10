// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

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
   DEFAULT_PACKAGE_LOAD_SCHEMA_CACHE_BYTES,
   PackageLoadPool,
   __setPackageLoadPoolForTests,
   getPackageLoadSchemaCacheBytes,
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

describe("getPackageLoadSchemaCacheBytes", () => {
   const original = process.env.PACKAGE_LOAD_SCHEMA_CACHE_BYTES;
   afterEach(() => {
      if (original === undefined) {
         delete process.env.PACKAGE_LOAD_SCHEMA_CACHE_BYTES;
      } else {
         process.env.PACKAGE_LOAD_SCHEMA_CACHE_BYTES = original;
      }
   });

   it("takes the default when unset or empty, as a chart rendering an empty value leaves it", () => {
      delete process.env.PACKAGE_LOAD_SCHEMA_CACHE_BYTES;
      expect(getPackageLoadSchemaCacheBytes()).toBe(
         DEFAULT_PACKAGE_LOAD_SCHEMA_CACHE_BYTES,
      );
      process.env.PACKAGE_LOAD_SCHEMA_CACHE_BYTES = " ";
      expect(getPackageLoadSchemaCacheBytes()).toBe(
         DEFAULT_PACKAGE_LOAD_SCHEMA_CACHE_BYTES,
      );
   });

   it("honors 0, which disables the cache, and other byte counts", () => {
      process.env.PACKAGE_LOAD_SCHEMA_CACHE_BYTES = "0";
      expect(getPackageLoadSchemaCacheBytes()).toBe(0);
      process.env.PACKAGE_LOAD_SCHEMA_CACHE_BYTES = "1048576";
      expect(getPackageLoadSchemaCacheBytes()).toBe(1048576);
   });

   it("refuses a value that is not a byte count rather than ignoring it", () => {
      for (const raw of ["16MiB", "-1", "1.5", "lots"]) {
         process.env.PACKAGE_LOAD_SCHEMA_CACHE_BYTES = raw;
         expect(() => getPackageLoadSchemaCacheBytes()).toThrow(
            /PACKAGE_LOAD_SCHEMA_CACHE_BYTES/,
         );
      }
   });
});

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
            environmentName: "env",
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
            environmentName: "env",
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
            environmentName: "env",
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

describe("PackageLoadPool (main-thread work budget)", () => {
   let tempDir: string;
   beforeEach(() => {
      tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "publisher-budget-"));
      fs.writeFileSync(
         path.join(tempDir, "publisher.json"),
         JSON.stringify({ name: "pkg" }),
      );
      fs.writeFileSync(
         path.join(tempDir, "orders.malloy"),
         `##! experimental { persistence composite_sources }

source: orders is duckdb.sql("""
  SELECT 1 AS order_id, 10 AS amount, 'A' AS category
""") extend {
  #@ preaggregate grain="category"
  measure: total is amount.sum()
  # bar_chart
  view: by_category is { group_by: category; aggregate: total }
}

#@ persist name="by_category_table"
source: by_category_table is orders -> by_category
`,
      );
   });
   afterEach(() => {
      fs.rmSync(tempDir, { recursive: true, force: true });
   });

   async function loadWith(budget?: number) {
      const pool = new PackageLoadPool(
         1,
         undefined,
         budget === undefined ? {} : { mainThreadWorkBudgetMs: budget },
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
         return await pool.loadPackage({
            packagePath: tempDir,
            packageName: "pkg",
            environmentName: "env",
            malloyConfig,
            defaultConnectionName: "duckdb",
            computeBuildPlan: true,
            withPreaggregateCompanions: true,
         });
      } finally {
         await pool.shutdown();
         await duckdb.close();
      }
   }

   it("does the main thread's work within the budget", async () => {
      const outcome = await loadWith();
      expect(outcome.buildPlan?.ok).toBe(true);
      expect(outcome.models[0].renderTagResults?.length).toBe(1);
      // The render-tag check reads the schema, not the SQL.
      expect(outcome.models[0].renderTagResults?.[0].result).not.toHaveProperty(
         "sql",
      );
      expect(outcome.models[0].preaggregateCompanion?.modelDef).toBeDefined();
   });

   it("leaves the main thread's work to it past the budget, and the load still succeeds", async () => {
      const outcome = await loadWith(0);
      expect(outcome.models[0].compilationError).toBeUndefined();
      expect(outcome.buildPlan).toBeUndefined();
      expect(outcome.models[0].renderTagResults).toBeUndefined();
      expect(outcome.models[0].preaggregateCompanion).toBeUndefined();
   });
});

describe("PackageLoadPool (dispatch)", () => {
   it("spreads a burst of loads across workers that are still starting", async () => {
      const pool = new PackageLoadPool(
         3,
         new URL("./test_fixtures/slow_ready_worker.ts", import.meta.url),
      );
      try {
         const { MalloyConfig } = await import("@malloydata/malloy");
         const outcomes = await Promise.all(
            [1, 2, 3].map((n) =>
               pool.loadPackage({
                  packagePath: `/nowhere/pkg-${n}`,
                  packageName: `pkg-${n}`,
                  environmentName: "env",
                  malloyConfig: new MalloyConfig({ connections: {} }),
                  defaultConnectionName: "duckdb",
               }),
            ),
         );
         expect(pool.size).toBe(3);
         expect(new Set(outcomes.map((o) => o.packageMetadata.name)).size).toBe(
            3,
         );
      } finally {
         await pool.shutdown();
      }
   });
});

describe("PackageLoadPool (shutdown)", () => {
   it("rejects loadPackage() after shutdown()", async () => {
      const { MalloyConfig } = await import("@malloydata/malloy");
      const pool = new PackageLoadPool(1);
      await pool.shutdown();
      await expect(
         pool.loadPackage({
            packagePath: "/tmp/nowhere",
            packageName: "nowhere",
            environmentName: "env",
            malloyConfig: new MalloyConfig({ connections: {} }),
            defaultConnectionName: "duckdb",
         }),
      ).rejects.toThrow("shutting down");
   });
});
