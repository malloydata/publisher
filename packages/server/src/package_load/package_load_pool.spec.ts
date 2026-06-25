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
import type { Connection } from "@malloydata/malloy";
import { BaseConnection } from "@malloydata/malloy/connection";
import { ConnectionAuthError, ConnectionError } from "../errors";

/**
 * A non-duckdb connection whose schema introspection always fails the same
 * way `PublisherConnection` does for an expired token: it THROWS, and Malloy's
 * `BaseConnection` catches that into `result.errors[table]` as a string. This
 * lets the real-worker tests drive the pool's failure classification (and the
 * compile cascade it short-circuits) without a live remote data plane.
 */
class FlakyConnection extends BaseConnection {
   constructor(private readonly failure: Error) {
      super();
   }
   get name(): string {
      return "flaky";
   }
   get dialectName(): string {
      // Borrow duckdb's dialect so table paths validate and the compiler can
      // reach the schema-fetch step (where introspection then fails).
      return "duckdb";
   }
   getDigest(): string {
      return "flaky-digest";
   }
   async fetchTableSchema(): Promise<never> {
      throw this.failure;
   }
   async fetchSelectSchema(): Promise<never> {
      throw this.failure;
   }
   async runSQL(): Promise<never> {
      throw new Error("FlakyConnection.runSQL is not used by compile tests");
   }
}

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

   // ───────────────────────────────────────────────────────────────
   // Connection introspection failures (the DX bug this fixes)
   //
   // A model that references a flaky non-duckdb connection. When schema
   // introspection fails, the source can't resolve, so every field
   // referencing it would normally emit its own "X is not defined" — the
   // misleading cascade. These tests assert auth/transport failures are
   // short-circuited into one connection error, while a genuine
   // table-not-found is left to compile as a normal model error.
   // ───────────────────────────────────────────────────────────────

   const FLAKY_MODEL = `source: sales is flaky.table('analytics.orders') extend {
  dimension: item is customer_id
  dimension: venue is order_status
  measure: total is order_total.sum()
}
run: sales -> { group_by: item, venue; aggregate: total }`;

   async function buildConfigWithFlaky(failure: Error): Promise<{
      malloyConfig: import("@malloydata/malloy").MalloyConfig;
      duckdb: { close: () => Promise<void> };
   }> {
      const { MalloyConfig, FixedConnectionMap } = await import(
         "@malloydata/malloy"
      );
      const { DuckDBConnection } = await import("@malloydata/db-duckdb");
      const duckdb = new DuckDBConnection("duckdb", ":memory:");
      const flaky = new FlakyConnection(failure);
      const connections = new FixedConnectionMap(
         new Map<string, Connection>([
            ["duckdb", duckdb as unknown as Connection],
            ["flaky", flaky as unknown as Connection],
         ]),
         "duckdb",
      );
      const malloyConfig = new MalloyConfig({ connections: {} });
      malloyConfig.wrapConnections(() => connections);
      return { malloyConfig, duckdb };
   }

   /** Load the flaky-model package and return the error it rejects with. */
   async function loadFlakyPackageExpectingRejection(
      failure: Error,
   ): Promise<Error> {
      writeManifest(tempDir);
      fs.writeFileSync(path.join(tempDir, "sales.malloy"), FLAKY_MODEL);
      const { malloyConfig, duckdb } = await buildConfigWithFlaky(failure);
      try {
         let caught: unknown;
         try {
            await pool.loadPackage({
               packagePath: tempDir,
               packageName: "pkg",
               malloyConfig,
               defaultConnectionName: "duckdb",
            });
         } catch (e) {
            caught = e;
         }
         if (!(caught instanceof Error)) {
            throw new Error("expected loadPackage to reject, but it resolved");
         }
         return caught;
      } finally {
         await duckdb.close();
      }
   }

   it("auth failure (401) surfaces ONE ConnectionAuthError, not the cascade", async () => {
      const err = await loadFlakyPackageExpectingRejection(
         new Error("Request failed with status code 401"),
      );
      expect(err).toBeInstanceOf(ConnectionAuthError);
      expect(err.message).toContain("flaky");
      expect(err.message).toContain("analytics.orders");
      expect(err.message).toContain("401");
      // The derivative cascade must NOT leak into the surfaced diagnostic.
      expect(err.message).not.toContain("is not defined");
      expect(err.message).not.toContain("import reference failure");
   });

   it("transport failure (503) surfaces ONE ConnectionError", async () => {
      const err = await loadFlakyPackageExpectingRejection(
         new Error("Request failed with status code 503"),
      );
      expect(err).toBeInstanceOf(ConnectionError);
      expect(err.message).toContain("503");
      expect(err.message).not.toContain("is not defined");
   });

   it("not-found (404) is NOT short-circuited — normal model error preserved", async () => {
      writeManifest(tempDir);
      fs.writeFileSync(path.join(tempDir, "sales.malloy"), FLAKY_MODEL);
      const { malloyConfig, duckdb } = await buildConfigWithFlaky(
         new Error("Request failed with status code 404"),
      );
      try {
         // Resolves (does NOT reject): the failure stays an in-band per-model
         // compilation error, exactly as a genuine modeling mistake would.
         const outcome = await pool.loadPackage({
            packagePath: tempDir,
            packageName: "pkg",
            malloyConfig,
            defaultConnectionName: "duckdb",
         });
         expect(outcome.models).toHaveLength(1);
         expect(outcome.models[0].compilationError).toBeDefined();
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
