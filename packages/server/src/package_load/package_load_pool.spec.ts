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
   spyOn,
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
   handleWorkerLog,
} from "./package_load_pool";
import { PACKAGE_LOAD_PROTOCOL_VERSION } from "./protocol";
import { PackageLoadProtocolError } from "./protocol_decode";
import { failsFirstStartMarker } from "./test_fixtures/fails_first_start_marker";
import { internalErrorToHttpError } from "../errors";
import { loadOutcomeOrThrow } from "../service/package_load_gates";
import { installSettings, withSettings } from "../test_helpers/settings_env";
import { writeTempPackage } from "../test_helpers/temp_package";

// ──────────────────────────────────────────────────────────────────────
// getPackageLoadWorkerCount — env var parsing
// ──────────────────────────────────────────────────────────────────────

describe("getPackageLoadWorkerCount", () => {
   const workersFrom = (raw: string) =>
      withSettings({ env: { PACKAGE_LOAD_WORKERS: raw } }, () =>
         getPackageLoadWorkerCount(),
      );
   const refusal = (raw: string) =>
      `packageLoad.workers from PACKAGE_LOAD_WORKERS: expected an integer of at least 1, got "${raw}"`;

   it("defaults to 1 worker when env unset", () => {
      expect(withSettings({}, () => getPackageLoadWorkerCount())).toBe(1);
   });

   it("throws when env value is non-numeric", () => {
      expect(() => workersFrom("not-a-number")).toThrow(
         refusal("not-a-number"),
      );
   });

   it("throws when env value is negative", () => {
      expect(() => workersFrom("-2")).toThrow(refusal("-2"));
   });

   it("throws when env value is 0 (no in-process fallback)", () => {
      expect(() => workersFrom("0")).toThrow(refusal("0"));
   });

   it("honors positive overrides", () => {
      expect(workersFrom("4")).toBe(4);
   });
});

// ──────────────────────────────────────────────────────────────────────
// PackageLoadPool constructor validation
// ──────────────────────────────────────────────────────────────────────

describe("getPackageLoadSchemaCacheBytes", () => {
   const cacheBytesFrom = (raw?: string) =>
      withSettings(
         {
            env:
               raw === undefined
                  ? {}
                  : { PACKAGE_LOAD_SCHEMA_CACHE_BYTES: raw },
         },
         () => getPackageLoadSchemaCacheBytes(),
      );

   it("takes the default when unset or empty, as a chart rendering an empty value leaves it", () => {
      expect(cacheBytesFrom()).toBe(DEFAULT_PACKAGE_LOAD_SCHEMA_CACHE_BYTES);
      expect(cacheBytesFrom(" ")).toBe(DEFAULT_PACKAGE_LOAD_SCHEMA_CACHE_BYTES);
   });

   it("honors 0, which disables the cache, and other byte counts", () => {
      expect(cacheBytesFrom("0")).toBe(0);
      expect(cacheBytesFrom("1048576")).toBe(1048576);
   });

   it("refuses a value that is not a byte count rather than ignoring it", () => {
      for (const raw of ["16MiB", "-1", "1.5", "lots"]) {
         expect(() => cacheBytesFrom(raw)).toThrow(
            `packageLoad.schemaCacheBytes from PACKAGE_LOAD_SCHEMA_CACHE_BYTES: expected an integer of at least 0, got "${raw}"`,
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

   it("compiles each model over the load's shared imports, keeping its own problems and data styles (#1566)", async () => {
      // index imports a then b; both import base. index, a and b are each
      // compiled once with their imports from the load's cache.
      writeManifest(tempDir);
      const write = (rel: string, text: string) =>
         fs.writeFileSync(path.join(tempDir, rel), text);
      write("base_styles.json", '{"shared":{"renderer":"base"}}');
      write("b_styles.json", '{"shared":{"renderer":"b"}}');
      write(
         "base.malloy",
         `--! styles base_styles.json
source: base is duckdb.sql("select 1 as a") extend {
  dimension: nothing is null
}`,
      );
      write("a.malloy", 'import "base.malloy"\nsource: a is base\n');
      write(
         "b.malloy",
         '--! styles b_styles.json\nimport "base.malloy"\nsource: b is base\n',
      );
      write(
         "index.malloy",
         `import "a.malloy"
import "b.malloy"
source: top is a extend { dimension: also_nothing is null }`,
      );

      const { malloyConfig, duckdb } = await buildConfig();
      try {
         const outcome = await pool.loadPackage({
            packagePath: tempDir,
            packageName: "pkg",
            environmentName: "env",
            malloyConfig,
            defaultConnectionName: "duckdb",
            collectProblems: true,
         });
         const byPath = new Map(outcome.models.map((m) => [m.modelPath, m]));
         expect([...byPath.keys()].sort()).toEqual([
            "a.malloy",
            "b.malloy",
            "base.malloy",
            "index.malloy",
         ]);
         for (const m of outcome.models) {
            expect(m.compilationError).toBeUndefined();
         }
         // base, a and b are imported, so stored; a and b each get base from
         // the store, and index gets a and b.
         expect(outcome.timings.translationsStored).toBe(3);
         expect(outcome.timings.translationsReused).toBe(4);
         // As a Runtime of the model's own read them: its file, then its
         // imports in the order Malloy reads them, later ones winning.
         expect(byPath.get("base.malloy")!.dataStyles).toEqual({
            shared: { renderer: "base" },
         });
         expect(byPath.get("a.malloy")!.dataStyles).toEqual({
            shared: { renderer: "base" },
         });
         expect(byPath.get("b.malloy")!.dataStyles).toEqual({
            shared: { renderer: "base" },
         });
         // index reads a, b, then base.
         expect(byPath.get("index.malloy")!.dataStyles).toEqual({
            shared: { renderer: "base" },
         });
         const warningsAt = (modelPath: string) =>
            (byPath.get(modelPath)!.problems ?? [])
               .filter(
                  (p) =>
                     (p as { code?: string }).code ===
                     "null-typed-field-definition",
               )
               .map((p) =>
                  path.basename((p as { at?: { url?: string } }).at?.url ?? ""),
               );
         // Each file's warning is reported by its own model; an importer that
         // took base from the cache reports only its own (the package compile
         // dedupes them across models anyway).
         expect(warningsAt("base.malloy")).toEqual(["base.malloy"]);
         expect(warningsAt("index.malloy")).toEqual(["index.malloy"]);
      } finally {
         await duckdb.close();
      }
   });

   it("serves a model the same whoever imports it (#1566)", async () => {
      // Importing a persistable source makes the translator record each
      // source's `#@ persist` flag on the imported model's registry entry. The
      // import must not reach the imported model's own result.
      const base = `source: facts is duckdb.sql("select 1 as a") -> { group_by: a }\n`;
      const importer = `import "base.malloy"\nsource: top is facts extend { measure: n is count() }\n`;
      const { malloyConfig, duckdb } = await buildConfig();
      const baseModelDef = async (withImporter: boolean) => {
         const dir = fs.mkdtempSync(path.join(os.tmpdir(), "publisher-alone-"));
         try {
            writeManifest(dir);
            fs.writeFileSync(path.join(dir, "base.malloy"), base);
            if (withImporter) {
               fs.writeFileSync(path.join(dir, "importer.malloy"), importer);
            }
            const outcome = await pool.loadPackage({
               packagePath: dir,
               packageName: "pkg",
               environmentName: "env",
               malloyConfig,
               defaultConnectionName: "duckdb",
            });
            for (const m of outcome.models) {
               expect(m.compilationError).toBeUndefined();
            }
            const model = outcome.models.find(
               (m) => m.modelPath === "base.malloy",
            );
            // Malloy names each compile's query sources with a fresh UUID.
            return (
               JSON.stringify(model?.modelDef)
                  // The directory appears as a path and as a file URL.
                  .replaceAll(path.basename(dir), "<pkg>")
                  .replace(
                     /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/g,
                     "<uuid>",
                  )
            );
         } finally {
            fs.rmSync(dir, { recursive: true, force: true });
         }
      };
      try {
         expect(await baseModelDef(true)).toBe(await baseModelDef(false));
      } finally {
         await duckdb.close();
      }
   });

   it("merges a model's data styles in the order Malloy reads its imports (#1566)", async () => {
      // root imports b then c; b's subtree (d, then e) is read before c's (f),
      // so f, read last, wins. A level-by-level order would read e last.
      writeManifest(tempDir);
      const write = (rel: string, text: string) =>
         fs.writeFileSync(path.join(tempDir, rel), text);
      const table = `duckdb.sql("select 1 as x")`;
      write("e.json", '{"k":{"renderer":"line_chart"}}');
      write("f.json", '{"k":{"renderer":"bar_chart"}}');
      write(
         "root.malloy",
         'import "b.malloy"\nimport "c.malloy"\nsource: r is b\n',
      );
      write("b.malloy", 'import "d.malloy"\nsource: b is d\n');
      write("d.malloy", 'import "e.malloy"\nsource: d is e\n');
      write("e.malloy", `--! styles e.json\nsource: e is ${table}\n`);
      write("c.malloy", 'import "f.malloy"\nsource: c is f\n');
      write("f.malloy", `--! styles f.json\nsource: f is ${table}\n`);

      const { malloyConfig, duckdb } = await buildConfig();
      try {
         const outcome = await pool.loadPackage({
            packagePath: tempDir,
            packageName: "pkg",
            environmentName: "env",
            malloyConfig,
            defaultConnectionName: "duckdb",
         });
         const styles = (modelPath: string) =>
            outcome.models.find((m) => m.modelPath === modelPath)?.dataStyles;
         expect(styles("root.malloy")).toEqual({
            k: { renderer: "bar_chart" },
         });
         expect(styles("b.malloy")).toEqual({ k: { renderer: "line_chart" } });
         expect(styles("c.malloy")).toEqual({ k: { renderer: "bar_chart" } });
      } finally {
         await duckdb.close();
      }
   });

   it("keeps an imported file's data styles when its path is not plain ASCII (#1566)", async () => {
      // A file is read under its encoded URL; a modelDef's dependencies name
      // it decoded. Both must find its styles.
      writeManifest(tempDir);
      fs.mkdirSync(path.join(tempDir, "my models"));
      fs.writeFileSync(
         path.join(tempDir, "my models", "styles.json"),
         '{"k":{"renderer":"bar_chart"}}',
      );
      fs.writeFileSync(
         path.join(tempDir, "my models", "bäse.malloy"),
         `--! styles styles.json\nsource: base is duckdb.sql("select 1 as x")\n`,
      );
      for (const importer of ["root.malloy", "other.malloy"]) {
         fs.writeFileSync(
            path.join(tempDir, importer),
            `import "my models/bäse.malloy"\nsource: s is base\n`,
         );
      }
      const { malloyConfig, duckdb } = await buildConfig();
      try {
         const outcome = await pool.loadPackage({
            packagePath: tempDir,
            packageName: "pkg",
            environmentName: "env",
            malloyConfig,
            defaultConnectionName: "duckdb",
         });
         expect(outcome.models.map((m) => m.modelPath).sort()).toEqual([
            "my models/bäse.malloy",
            "other.malloy",
            "root.malloy",
         ]);
         for (const m of outcome.models) {
            expect(m.compilationError).toBeUndefined();
            expect(m.dataStyles).toEqual({ k: { renderer: "bar_chart" } });
         }
      } finally {
         await duckdb.close();
      }
   });

   it("reports an imported non-model file's warnings from each importer, as without the cache (#1566)", async () => {
      // `.lib/x.malloy` is not a package model (a dotfile), so only its
      // importers report its warnings. b imports a, so it compiles after a has
      // translated x; it must translate x itself rather than take a's.
      writeManifest(tempDir);
      fs.mkdirSync(path.join(tempDir, ".lib"));
      fs.writeFileSync(
         path.join(tempDir, ".lib", "x.malloy"),
         `source: x is duckdb.sql("select 1 as a") extend {\n  dimension: nothing is null\n}\n`,
      );
      fs.writeFileSync(
         path.join(tempDir, "a.malloy"),
         `import ".lib/x.malloy"\nsource: a is x\n`,
      );
      fs.writeFileSync(
         path.join(tempDir, "b.malloy"),
         `import "a.malloy"\nimport ".lib/x.malloy"\nsource: b is a\n`,
      );
      const { malloyConfig, duckdb } = await buildConfig();
      try {
         const outcome = await pool.loadPackage({
            packagePath: tempDir,
            packageName: "pkg",
            environmentName: "env",
            malloyConfig,
            defaultConnectionName: "duckdb",
            collectProblems: true,
         });
         const warnedFrom = (modelPath: string) =>
            (
               outcome.models.find((m) => m.modelPath === modelPath)
                  ?.problems ?? []
            )
               .map((p) => (p as { at?: { url?: string } }).at?.url ?? "")
               .filter((url) => url.endsWith("/.lib/x.malloy")).length;
         expect(outcome.models.map((m) => m.modelPath).sort()).toEqual([
            "a.malloy",
            "b.malloy",
         ]);
         expect(warnedFrom("a.malloy")).toBeGreaterThan(0);
         expect(warnedFrom("b.malloy")).toBeGreaterThan(0);
      } finally {
         await duckdb.close();
      }
   });

   it("fails the models of an import cycle, and only those, without holding the load (#1566)", async () => {
      // a -> b -> c -> a. Malloy has no import-cycle check of its own (the
      // translator overflows its stack), so each of these fails as it did
      // before the job cache; nothing is cached from them, and the load ends.
      writeManifest(tempDir);
      const write = (rel: string, text: string) =>
         fs.writeFileSync(path.join(tempDir, rel), text);
      const src = (name: string) =>
         `source: ${name} is duckdb.sql("select 1 as a")\n`;
      write("a.malloy", `import "b.malloy"\n${src("a")}`);
      write("b.malloy", `import "c.malloy"\n${src("b")}`);
      write("c.malloy", `import "a.malloy"\n${src("c")}`);
      write("into_cycle.malloy", `import "a.malloy"\n${src("into")}`);
      write("free.malloy", src("free"));

      const { malloyConfig, duckdb } = await buildConfig();
      try {
         const outcome = await pool.loadPackage({
            packagePath: tempDir,
            packageName: "pkg",
            environmentName: "env",
            malloyConfig,
            defaultConnectionName: "duckdb",
         });
         const failed = outcome.models
            .filter((m) => m.compilationError !== undefined)
            .map((m) => m.modelPath)
            .sort();
         expect(failed).toEqual([
            "a.malloy",
            "b.malloy",
            "c.malloy",
            "into_cycle.malloy",
         ]);
         const free = outcome.models.find((m) => m.modelPath === "free.malloy");
         expect(free?.compilationError).toBeUndefined();
         expect(free?.modelDef).toBeDefined();
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
      tempDir = writeTempPackage({}, { prefix: "publisher-budget-" }).dir;
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
            withEntities: true,
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
      // A v1 rollup is the rollup leg's: no composite companion is compiled.
      expect(outcome.models[0].preaggregateCompanion?.modelDef).toBeUndefined();
      const entities = outcome.models[0].entities;
      expect(entities?.status).toBe("planned");
      if (entities?.status !== "planned") return;
      expect(entities.relativePath).toBe(outcome.models[0].modelPath);
      expect(entities.packageRoot).toBe(tempDir);
      // No `explores`, so no boundary: nothing is taken provisionally.
      expect(entities.provisional).toBe(false);
      expect(entities.metadata.map((s) => s.sourceName)).toEqual([
         "orders",
         "by_category_table",
      ]);
   });

   it("leaves the render-tag results, companion and plan to the main thread past the budget", async () => {
      const outcome = await loadWith(0);
      expect(outcome.models[0].compilationError).toBeUndefined();
      // No target reached, so the main thread prepares all of them.
      expect(outcome.models[0].renderTagResults).toEqual([]);
      expect(outcome.models[0].preaggregateCompanion).toBeUndefined();
      expect(outcome.buildPlan).toBeUndefined();
      // The main thread plans the entities itself.
      expect(outcome.models[0].entities).toBeUndefined();
   });
});

describe("PackageLoadPool (work for the main thread)", () => {
   let tempDir: string;
   beforeEach(() => {
      tempDir = writeTempPackage({}, { prefix: "publisher-prep-" }).dir;
   });
   afterEach(() => {
      fs.rmSync(tempDir, { recursive: true, force: true });
   });

   /**
    * A pool and a DuckDB connection whose table-schema fetches, as this
    * thread answers the worker's, are recorded and held up by `hold`.
    */
   async function setUp(
      hold: (tables: string[]) => Promise<void>,
      options: { jobTimeoutMs?: number; mainThreadWorkBudgetMs?: number } = {},
   ) {
      const pool = new PackageLoadPool(1, undefined, options);
      const { MalloyConfig, FixedConnectionMap } = await import(
         "@malloydata/malloy"
      );
      const { DuckDBConnection } = await import("@malloydata/db-duckdb");
      const duckdb = new DuckDBConnection("duckdb", ":memory:");
      const tableFetches: string[][] = [];
      const fetch = duckdb.fetchSchemaForTables.bind(duckdb);
      duckdb.fetchSchemaForTables = async (tables, fetchOptions) => {
         const paths = Object.values(tables);
         tableFetches.push(paths);
         await hold(paths);
         return fetch(tables, fetchOptions);
      };
      const malloyConfig = new MalloyConfig({ connections: {} });
      malloyConfig.wrapConnections(
         () => new FixedConnectionMap(new Map([["duckdb", duckdb]]), "duckdb"),
      );
      const load = (computeBuildPlan: boolean) =>
         pool.loadPackage({
            packagePath: tempDir,
            packageName: "pkg",
            environmentName: "env",
            malloyConfig,
            defaultConnectionName: "duckdb",
            computeBuildPlan,
            withPreaggregateCompanions: true,
         });
      const tearDown = async () => {
         await pool.shutdown();
         await duckdb.close();
      };
      return { duckdb, tableFetches, load, tearDown };
   }

   const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

   it("starts a model once its own imports have compiled, not after unrelated ones (#1566)", async () => {
      // slow's table answers only once importer has asked for its own, and
      // importer imports only base. Were importer to wait for every model
      // compiled before it, slow included, the job would time out.
      fs.writeFileSync(
         path.join(tempDir, "base.malloy"),
         `source: base is duckdb.sql("select 1 as a")\n`,
      );
      fs.writeFileSync(
         path.join(tempDir, "slow.malloy"),
         `source: slow is duckdb.table('held_slow_t')\n`,
      );
      fs.writeFileSync(
         path.join(tempDir, "importer.malloy"),
         `import "base.malloy"\nsource: mine is duckdb.table('held_mine_t')\n`,
      );
      let importerAsked!: () => void;
      const importerAsks = new Promise<void>((r) => (importerAsked = r));
      const { duckdb, load, tearDown } = await setUp(
         (tables) => {
            if (tables.includes("held_mine_t")) importerAsked();
            return tables.includes("held_slow_t")
               ? importerAsks
               : Promise.resolve();
         },
         { jobTimeoutMs: 20000 },
      );
      try {
         await duckdb.runSQL(
            "create or replace table held_slow_t as select 1 as a",
         );
         await duckdb.runSQL(
            "create or replace table held_mine_t as select 1 as b",
         );
         const outcome = await load(false);
         for (const m of outcome.models) {
            expect(m.compilationError).toBeUndefined();
         }
      } finally {
         await tearDown();
      }
   });

   it("logs a job's entries on this thread as they are written, so a job that times out keeps them", async () => {
      // A malformed `#@ persist` tag is reported while the worker collects
      // the plan; the other model waits on a table forever, so the job times
      // out after that report.
      fs.writeFileSync(
         path.join(tempDir, "malformed.malloy"),
         `##! experimental.persistence
source: base is duckdb.sql("select 1 as a")
#@ persist name="t" (
source: t is base -> { group_by: a }
`,
      );
      fs.writeFileSync(
         path.join(tempDir, "stuck.malloy"),
         `source: stuck is duckdb.table('never_answered')\n`,
      );
      const { tearDown, load } = await setUp(
         (tables) =>
            tables.includes("never_answered")
               ? new Promise<void>(() => {})
               : Promise.resolve(),
         // Room for a fresh worker to start and report the malformed tag
         // before the stuck model times the job out; 1.5s was not enough on
         // Windows CI.
         { jobTimeoutMs: 5000 },
      );
      const { logger } = await import("../logger");
      const logSpy = spyOn(logger, "log");
      try {
         await expect(load(true)).rejects.toThrow(/timed out/);
         // winston folds the entry's `message` field into its message, as it
         // does for the same call on this thread. (The console format colours
         // `level` in place, so the entry is found by its message.)
         const reported = logSpy.mock.calls
            .map(([entry]) => entry as unknown as Record<string, unknown>)
            .find((entry) =>
               String(entry.message).startsWith("Persist annotation issue"),
            );
         expect(reported).toMatchObject({
            packagePath: tempDir,
            modelPath: "malformed.malloy",
         });
      } finally {
         logSpy.mockRestore();
         await tearDown();
      }
   });

   it("stops preparing for this thread once a model fails in a load that wants a plan", async () => {
      fs.writeFileSync(
         path.join(tempDir, "good.malloy"),
         `source: good is duckdb.table('slow_t') extend {
  # bar_chart
  view: by_a is { group_by: a }
}
`,
      );
      // The good model's compile waits on its table, so the broken one has
      // failed by the time the good one is ready for its share.
      const { duckdb, load, tearDown } = await setUp((tables) =>
         tables.includes("slow_t") ? sleep(500) : Promise.resolve(),
      );
      try {
         await duckdb.runSQL("create table slow_t as select 1 as a");
         const brokenPath = path.join(tempDir, "broken.malloy");
         fs.writeFileSync(
            brokenPath,
            `source: broken is duckdb.sql("select 1 as a") extend {
  dimension: b is no_such_field
}
`,
         );
         const outcome = await load(true);
         const byPath = new Map(outcome.models.map((m) => [m.modelPath, m]));
         expect(byPath.get("broken.malloy")?.compilationError).toBeDefined();
         expect(byPath.get("good.malloy")?.compilationError).toBeUndefined();
         expect(byPath.get("good.malloy")?.renderTagResults).toBeUndefined();
         expect(outcome.buildPlan).toBeUndefined();

         // Without the broken model, the good one's results are prepared.
         fs.rmSync(brokenPath);
         const alone = await load(true);
         expect(alone.models[0].renderTagResults?.length).toBe(1);
      } finally {
         await tearDown();
      }
   });

   it("asks this thread once for a table every model waits on, sharing a failed fetch's error", async () => {
      for (let i = 0; i < 5; i++) {
         fs.writeFileSync(
            path.join(tempDir, `m${i}.malloy`),
            `source: s${i} is duckdb.table('missing_t')\n`,
         );
      }
      // Held long enough for every model's compile to ask for the table
      // while the first request is still out.
      const { tableFetches, load, tearDown } = await setUp((tables) =>
         tables.includes("missing_t") ? sleep(300) : Promise.resolve(),
      );
      try {
         const outcome = await load(false);
         expect(outcome.models).toHaveLength(5);
         for (const model of outcome.models) {
            expect(model.compilationError?.message).toContain("missing_t");
         }
         expect(
            tableFetches.filter((paths) => paths.includes("missing_t")),
         ).toHaveLength(1);
      } finally {
         await tearDown();
      }
   });

   it("loads a package whose compile fits the job timeout, with the work for this thread on top", async () => {
      // With the worker's schema cache off, the compile waits 1000 ms on its
      // table and the rollup companion's compile then waits 3500 ms on it again. The
      // compile fits a 4000 ms timeout with room to spare before the worker's
      // deadline at three quarters of it (3000 ms), and the job, at 4500 ms or
      // more, runs past the timeout itself. The companion ends past the
      // deadline, and the plan parts read from it need no recompile of the
      // model, so the worker still collects them rather than leaving the whole
      // plan, and a compile of every model, to this thread.
      fs.writeFileSync(
         path.join(tempDir, "orders.malloy"),
         `##! experimental { persistence composite_sources }

source: orders is duckdb.table('limit_t') extend {
  #@ preaggregate grain="category"
  measure: total is amount.sum()
}
`,
      );
      const cacheOff = installSettings({
         env: { PACKAGE_LOAD_SCHEMA_CACHE_BYTES: "0" },
      });
      let limitFetches = 0;
      const { duckdb, load, tearDown } = await setUp(
         (tables) => {
            if (!tables.includes("limit_t")) return Promise.resolve();
            limitFetches += 1;
            return sleep(limitFetches === 1 ? 1000 : 3500);
         },
         { jobTimeoutMs: 4000 },
      );
      try {
         await duckdb.runSQL(
            "create or replace table limit_t as select 10 as amount, 'A' as category",
         );
         const outcome = await load(true);
         expect(outcome.models[0].compilationError).toBeUndefined();
         // No composite companion: the plan's own rollup companion compiles.
         expect(outcome.models[0].preaggregateCompanion).toEqual({});
         expect(outcome.buildPlan?.ok).toBe(true);
      } finally {
         await tearDown();
         cacheOff.restore();
      }
   }, 20000);

   it("derives the plan from the companion the worker compiled for this thread, not from a second compile", async () => {
      fs.writeFileSync(
         path.join(tempDir, "orders.malloy"),
         `##! experimental { persistence composite_sources }

source: orders is duckdb.table('shared_t') extend {
  #@ preaggregate grain="category"
  measure: total is amount.sum()
}
`,
      );
      // With the worker's schema cache off, every compile fetches the table:
      // the model's, the rollup companion's, and a second one's if the plan
      // compiled its own.
      const cacheOff = installSettings({
         env: { PACKAGE_LOAD_SCHEMA_CACHE_BYTES: "0" },
      });
      const { duckdb, tableFetches, load, tearDown } = await setUp(() =>
         Promise.resolve(),
      );
      try {
         await duckdb.runSQL(
            "create or replace table shared_t as select 10 as amount, 'A' as category",
         );
         const outcome = await load(true);
         // No composite companion: the plan's own rollup companion compiles.
         expect(outcome.models[0].preaggregateCompanion).toEqual({});
         expect(outcome.buildPlan?.ok).toBe(true);
         expect(
            tableFetches.filter((paths) => paths.includes("shared_t")),
         ).toHaveLength(2);
      } finally {
         await tearDown();
         cacheOff.restore();
      }
   });

   it("answers a schema fetch that outlives its job's timeout without ending the process", async () => {
      // The answer reaches a worker the timeout has terminated. Under Bun 1.3
      // posting to it throws, and an unhandled throw there ends the process,
      // this test run included.
      fs.writeFileSync(
         path.join(tempDir, "late.malloy"),
         `source: late is duckdb.table('late_t')\n`,
      );
      // The held fetch outlasts the timeout by a wide margin, and the timeout
      // leaves the healthy load below room to start a fresh worker on a slow
      // runner: at 250ms it timed out on Windows CI.
      const { duckdb, load, tearDown } = await setUp(
         (tables) =>
            tables.includes("late_t") ? sleep(4000) : Promise.resolve(),
         { jobTimeoutMs: 1500 },
      );
      try {
         await duckdb.runSQL("create or replace table late_t as select 1 as a");
         await expect(load(false)).rejects.toThrow(/timed out/);
         // Past the moment the held fetch answers.
         await sleep(4000);
         fs.rmSync(path.join(tempDir, "late.malloy"));
         fs.writeFileSync(
            path.join(tempDir, "fine.malloy"),
            `source: fine is duckdb.sql("select 1 as a")\n`,
         );
         const outcome = await load(false);
         expect(outcome.models.map((m) => m.modelPath)).toEqual([
            "fine.malloy",
         ]);
      } finally {
         await tearDown();
      }
   });

   it("sends a load retried after a timeout to a new worker, not to the one that timed out", async () => {
      fs.writeFileSync(
         path.join(tempDir, "good.malloy"),
         `source: good is duckdb.sql("select 1 as a")\n`,
      );
      const stuckPath = path.join(tempDir, "stuck.malloy");
      fs.writeFileSync(
         stuckPath,
         `source: stuck is duckdb.table('never_answered')\n`,
      );
      const { load, tearDown } = await setUp(
         (tables) =>
            tables.includes("never_answered")
               ? new Promise<void>(() => {})
               : Promise.resolve(),
         // The retried load starts a fresh worker inside this budget; 500ms
         // was not enough on Windows CI.
         { jobTimeoutMs: 3000 },
      );
      try {
         // Retried as soon as the timeout is reported, while the worker that
         // timed out is still exiting.
         const retried = load(false).then(
            () => {
               throw new Error("The first load should have timed out");
            },
            (error: Error) => {
               expect(error.message).toMatch(/timed out/);
               fs.rmSync(stuckPath);
               return load(false);
            },
         );
         const outcome = await retried;
         expect(outcome.models.map((m) => m.modelPath)).toEqual([
            "good.malloy",
         ]);
      } finally {
         await tearDown();
      }
   });
});

describe("handleWorkerLog", () => {
   it("keeps an entry's fields when its message reads like a format string", async () => {
      const { logger } = await import("../logger");
      const write = spyOn(logger, "write");
      try {
         handleWorkerLog({
            type: "log",
            jobId: "job-1",
            packagePath: "/pkg",
            level: "warn",
            message: "Persist annotation issue near '%smith%'",
            meta: { modelPath: "m.malloy" },
         });
         expect(write.mock.calls[0]?.[0]).toMatchObject({
            message: "Persist annotation issue near '%smith%'",
            packagePath: "/pkg",
            modelPath: "m.malloy",
         });
      } finally {
         write.mockRestore();
      }
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

describe("PackageLoadPool (stopped workers)", () => {
   it("sends the loads queued behind a worker that fails to start to a new worker", async () => {
      const marker = failsFirstStartMarker();
      fs.rmSync(marker, { force: true });
      const pool = new PackageLoadPool(
         1,
         new URL(
            "./test_fixtures/fails_first_start_worker.ts",
            import.meta.url,
         ),
      );
      try {
         const { MalloyConfig } = await import("@malloydata/malloy");
         const results = await Promise.allSettled(
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
         // The load waiting on the failed worker fails with it; the two
         // queued behind it are served by its replacement.
         expect(results.map((r) => r.status)).toEqual([
            "rejected",
            "fulfilled",
            "fulfilled",
         ]);
      } finally {
         await pool.shutdown();
         fs.rmSync(marker, { force: true });
      }
   });

   it("drops an answer to a worker that has stopped rather than throwing", async () => {
      const pool = new PackageLoadPool(1);
      const post = (
         pool as unknown as {
            postToWorker: (pw: unknown, message: unknown) => void;
         }
      ).postToWorker.bind(pool);
      try {
         const terminated = {
            id: 1,
            exited: false,
            worker: {
               postMessage: () => {
                  throw new Error("Worker has been terminated");
               },
            },
         };
         expect(() => post(terminated, { type: "rpc-error" })).not.toThrow();
         let posted = 0;
         post(
            { id: 2, exited: true, worker: { postMessage: () => posted++ } },
            { type: "rpc-error" },
         );
         expect(posted).toBe(0);
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

describe("PackageLoadPool (protocol checks)", () => {
   let pool: PackageLoadPool;

   beforeAll(() => {
      pool = new PackageLoadPool(
         1,
         new URL("./test_fixtures/bad_result_worker.ts", import.meta.url),
      );
   });

   afterAll(async () => {
      await pool.shutdown();
   });

   async function load(packageName: string) {
      const { MalloyConfig } = await import("@malloydata/malloy");
      return pool.loadPackage({
         packagePath: `/nowhere/${packageName}`,
         packageName,
         environmentName: "env",
         malloyConfig: new MalloyConfig({ connections: {} }),
         defaultConnectionName: "duckdb",
      });
   }

   async function rejectionOf(job: Promise<unknown>): Promise<unknown> {
      return job.then(
         () => undefined,
         (e: unknown) => e,
      );
   }

   it("resolves a well-formed result on this protocol version unchanged", async () => {
      const outcome = await load("good");
      expect(outcome.packageMetadata.name).toBe("good");
      expect(outcome.models.map((m) => m.modelPath)).toEqual(["m.malloy"]);
   });

   it("rejects a result from a worker on another protocol version", async () => {
      const error = await rejectionOf(load("wrong-version"));
      expect(error).toBeInstanceOf(PackageLoadProtocolError);
      expect((error as Error).message).toContain(
         `protocol version ${PACKAGE_LOAD_PROTOCOL_VERSION + 1}, this server ${PACKAGE_LOAD_PROTOCOL_VERSION}`,
      );
   });

   it("rejects a result with no protocol version", async () => {
      await expect(load("no-version")).rejects.toThrow(
         /protocol version undefined/,
      );
   });

   it("rejects a result carrying a malformed model, naming the field", async () => {
      await expect(load("bad-model")).rejects.toThrow(
         /serialized model "m\.malloy" has sources of kind string; expected array/,
      );
   });

   it("keeps serving after a rejected result", async () => {
      await rejectionOf(load("bad-model"));
      const outcome = await load("good-again");
      expect(outcome.packageMetadata.name).toBe("good-again");
   });

   it("surfaces as an internal 500, not a retryable 503, through loadOutcomeOrThrow", async () => {
      const error = await rejectionOf(
         loadOutcomeOrThrow(load("wrong-version")),
      );
      expect(error).toBeInstanceOf(PackageLoadProtocolError);
      expect(
         internalErrorToHttpError(error as Error, { log: false }).status,
      ).toBe(500);
   });
});
