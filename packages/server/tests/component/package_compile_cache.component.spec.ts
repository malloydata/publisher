// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The per-package compile cache in the load worker: an unchanged file's
 * ModelDef (and what the load derives from it) is served across loads, and a
 * connection change (but not a token-only one), a changed data file or an
 * unload empties it.
 *
 * One pool for the file, as in package_worker_path.component.spec.ts.
 */
import { afterEach, beforeEach, describe, expect, it, spyOn } from "bun:test";
import { DuckDBConnection } from "@malloydata/db-duckdb";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import type { components } from "../../src/api";
import {
   dropPackageCompileCache,
   type LoadPackageOutcome,
   refreshPackageConnectionsEpoch,
} from "../../src/package_load/package_load_pool";
import { PackageController } from "../../src/controller/package.controller";
import type { EnvironmentStore } from "../../src/service/environment_store";
import { compileSource } from "../../src/service/compile_source";
import { buildEnvironmentMalloyConfig } from "../../src/service/connection";
import { ConnectionService } from "../../src/service/connection_service";
import { Environment } from "../../src/service/environment";
import { Package } from "../../src/service/package";
import { startFakePublisher } from "../../src/test_helpers/fake_publisher";
import { resetAuthorizeGuardTelemetryForTesting } from "../../src/authorize_metrics";
import { startMetricsHarness } from "../../src/test_helpers/metrics_harness";
import {
   duckdbMalloyConfig,
   usePackageLoadPool,
} from "../../src/test_helpers/temp_package";

type ApiConnection = components["schemas"]["Connection"];

const BASE = `--! styles base.styles.json
source: base is duckdb.sql("select 1 as id, 5 as n") extend {
  measure: total is n.sum()
}
`;
const LEAF = `import "base.malloy"
source: leaf is base extend {
  view: by_id is { group_by: id aggregate: total }
}
`;
const OTHER = `source: other is duckdb.sql("select 2 as id") extend {
  dimension: is_missing is id = null
}
`;

describe("package compile cache", () => {
   const workers = usePackageLoadPool();
   let tempDir: string;
   let packagePath: string;

   beforeEach(() => {
      tempDir = fs.mkdtempSync(
         path.join(os.tmpdir(), "publisher-compile-cache-"),
      );
      packagePath = path.join(tempDir, "pkg");
      fs.mkdirSync(packagePath);
      const files: Record<string, string> = {
         "publisher.json": JSON.stringify({ name: "pkg" }),
         "base.styles.json": JSON.stringify({ by_id: { renderer: "table" } }),
         "base.malloy": BASE,
         "leaf.malloy": LEAF,
         "other.malloy": OTHER,
      };
      for (const [name, text] of Object.entries(files)) {
         fs.writeFileSync(path.join(packagePath, name), text);
      }
   });

   let restoreLoads = () => {};

   afterEach(() => {
      restoreLoads();
      restoreLoads = () => {};
      fs.rmSync(tempDir, { recursive: true, force: true });
   });

   /** Every load's outcome, through the pool the file installed. */
   function recordLoads(): LoadPackageOutcome[] {
      const outcomes: LoadPackageOutcome[] = [];
      const loadPackage = workers.pool.loadPackage.bind(workers.pool);
      const spy = spyOn(workers.pool, "loadPackage").mockImplementation(
         async (request) => {
            const outcome = await loadPackage(request);
            outcomes.push(outcome);
            return outcome;
         },
      );
      restoreLoads = () => spy.mockRestore();
      return outcomes;
   }

   const hits = (outcome: LoadPackageOutcome) =>
      Object.fromEntries(
         outcome.models.map((m) => [m.modelPath, m.compileCacheHit]),
      );

   describe("in the worker", () => {
      let duckdb: DuckDBConnection;
      let environmentName: string;
      let load: (
         connectionsEpoch: number,
         extra?: { collectProblems?: boolean },
      ) => Promise<LoadPackageOutcome>;

      beforeEach(() => {
         // A fresh environment per test: the worker's caches outlive a test.
         environmentName = `env-${Math.random().toString(36).slice(2)}`;
         const connections = duckdbMalloyConfig();
         duckdb = connections.duckdb;
         load = (connectionsEpoch, extra = {}) =>
            workers.pool.loadPackage({
               packagePath,
               packageName: "pkg",
               environmentName,
               malloyConfig: connections.malloyConfig,
               defaultConnectionName: "duckdb",
               connectionsEpoch,
               ...extra,
            });
      });

      afterEach(async () => {
         await duckdb.close();
      });

      it("serves an unchanged tree's reload from the cache, with the same models", async () => {
         const cold = await load(1);
         expect(cold.timings.compileCacheHits).toBe(0);
         expect(cold.models.some((m) => m.derivationReused)).toBe(false);
         const warm = await load(1);
         expect(warm.timings.compileCacheHits).toBe(3);
         // What a load derives from each ModelDef is reused once the cache holds
         // the ModelDef that was derived; an importer's translation can replace
         // one entry on the cold load, so the steady state is the next reload.
         const steady = await load(1);
         expect(steady.models.map((m) => m.derivationReused)).toEqual([
            true,
            true,
            true,
         ]);
         // Nothing was translated, so nothing asked for a schema.
         expect(warm.timings.schemaCacheHits).toBe(0);
         expect(warm.timings.schemaFetchCount).toBe(0);
         // A hit still reads the files, so the text and styles still arrive.
         // Each load mints new reference ids, and Malloy memoizes a registry
         // entry's `persist` flag on the ModelDef the first time it reads it.
         const comparable = (outcome: LoadPackageOutcome) =>
            JSON.stringify(
               outcome.models.map(
                  ({
                     compileDurationMs: _d,
                     compileCacheHit: _h,
                     derivationReused: _r,
                     ...model
                  }) => model,
               ),
               (key, value) =>
                  key === "persist" && typeof value === "boolean"
                     ? undefined
                     : value,
            ).replace(/reference_id = \\"[0-9a-f-]+\\"/g, "reference_id");
         expect(comparable(warm)).toEqual(comparable(cold));
         const base = warm.models.find((m) => m.modelPath === "base.malloy");
         expect(base?.modelSourceText).toBe(BASE);
         expect(base?.dataStyles).toEqual({ by_id: { renderer: "table" } });
      });

      it("recompiles an edited file and its importers only", async () => {
         await load(1);
         fs.writeFileSync(
            path.join(packagePath, "base.malloy"),
            BASE.replace("}", "  measure: added_measure is count()\n}"),
         );
         const edited = await load(1);
         expect(hits(edited)).toEqual({
            "base.malloy": false,
            "leaf.malloy": false,
            "other.malloy": true,
         });
         // The derivation is redone for a new ModelDef, not reused.
         for (const model of edited.models.filter((m) => !m.compileCacheHit)) {
            expect(JSON.stringify(model.modelDef)).toContain("added_measure");
            expect(JSON.stringify(model.sourceInfos)).toContain(
               "added_measure",
            );
         }
      });

      it("empties the cache when the connections change", async () => {
         await load(1);
         const changed = await load(2);
         expect(changed.timings.compileCacheHits).toBe(0);
         expect((await load(2)).timings.compileCacheHits).toBe(3);
      });

      it("drops a package's cache when it is unloaded", async () => {
         await load(1);
         dropPackageCompileCache(environmentName, "pkg");
         expect((await load(1)).timings.compileCacheHits).toBe(0);
      });

      it("still reports a file's own problems at package scope on a warm cache", async () => {
         await load(1);
         const dryRun = await load(1, { collectProblems: true });
         const other = dryRun.models.find(
            (m) => m.modelPath === "other.malloy",
         );
         expect(other?.compileCacheHit).toBe(false);
         expect(
            (other?.problems as { message: string }[]).map((p) => p.message),
         ).toContain("Use 'is null' to check for NULL instead of '= null'");
      });
   });

   describe("with a publisher connection", () => {
      /** An environment whose `warehouse` connection is proxied to a fake publisher, with `load` timing each package load. */
      async function publisherEnvironment() {
         const previous = process.env.PUBLISHER_ALLOW_PROXY_CONNECTIONS;
         process.env.PUBLISHER_ALLOW_PROXY_CONNECTIONS = "true";
         const warehouse = new DuckDBConnection("warehouse", ":memory:");
         await warehouse.runSQL(
            "create or replace table compile_cache_orders as select 1 as id",
         );
         const remote = startFakePublisher(warehouse);
         const moved = startFakePublisher(warehouse);
         fs.writeFileSync(
            path.join(packagePath, "orders.malloy"),
            `source: orders is warehouse.table('compile_cache_orders')\n`,
         );
         let stored: ApiConnection = {
            name: "warehouse",
            type: "publisher",
            publisherConnection: {
               connectionUri: remote.connectionUri("warehouse"),
               accessToken: "old",
            },
         };
         const envConfig = buildEnvironmentMalloyConfig([stored], tempDir);
         const environment = new Environment(
            "publisher-env",
            tempDir,
            envConfig,
            envConfig.apiConnections,
         );
         const service = new ConnectionService({
            finishedInitialization: Promise.resolve(),
            publisherConfigIsFrozen: false,
            getEnvironment: async () => environment,
            updateConnection: async (next: ApiConnection) => {
               stored = next;
            },
            storageManager: {
               getRepository: () => ({
                  getEnvironmentByName: async () => ({ id: "env-id" }),
                  getConnectionByName: async () => ({ config: stored }),
               }),
            },
         } as unknown as ConstructorParameters<typeof ConnectionService>[0]);
         const outcomes = recordLoads();
         const packages: Package[] = [];
         return {
            warehouse,
            remote,
            moved,
            /** The orders fields the latest load compiled. */
            ordersFields: () =>
               JSON.stringify(
                  (
                     outcomes[outcomes.length - 1].models.find(
                        (m) => m.modelPath === "orders.malloy",
                     )?.modelDef as {
                        contents: {
                           orders: { fields: Array<{ name: string }> };
                        };
                     }
                  ).contents.orders.fields.map((f) => f.name),
               ),
            patch: (uri: string) =>
               service.updateConnection("publisher-env", "warehouse", {
                  publisherConnection: {
                     connectionUri: uri,
                     accessToken: "new",
                  },
               }),
            /** The timings of a fresh load of the package. */
            load: async () => {
               packages.push(
                  await Package.create(
                     "publisher-env",
                     "pkg",
                     packagePath,
                     () => environment.getEnvironmentMalloyConfig(),
                  ),
               );
               return outcomes[outcomes.length - 1].timings;
            },
            cleanup: async () => {
               dropPackageCompileCache("publisher-env", "pkg");
               for (const pkg of packages) {
                  await pkg.getMalloyConfig().releaseConnections();
               }
               await environment.getEnvironmentMalloyConfig().shutdown("close");
               remote.stop();
               moved.stop();
               await warehouse.close();
               if (previous === undefined) {
                  delete process.env.PUBLISHER_ALLOW_PROXY_CONNECTIONS;
               } else {
                  process.env.PUBLISHER_ALLOW_PROXY_CONNECTIONS = previous;
               }
            },
         };
      }

      it("keeps the cache across a token-only PATCH and empties it on a host change", async () => {
         const env = await publisherEnvironment();
         try {
            expect((await env.load()).compileCacheHits).toBe(0);
            await env.patch(env.remote.connectionUri("warehouse"));
            expect((await env.load()).compileCacheHits).toBe(4);
            await env.patch(env.moved.connectionUri("warehouse"));
            expect((await env.load()).compileCacheHits).toBe(0);
         } finally {
            await env.cleanup();
         }
      });

      it("a schema refresh reaches the warehouse, once, and picks up a new column", async () => {
         // Only the warehouse table: the sandbox duckdb is a new connection
         // on every load, so models over it always fetch.
         for (const model of ["base", "leaf", "other"]) {
            fs.rmSync(path.join(packagePath, `${model}.malloy`));
         }
         const env = await publisherEnvironment();
         const fetched = spyOn(env.warehouse, "fetchTableSchema");
         const edit = (n: number) =>
            fs.writeFileSync(
               path.join(packagePath, "orders.malloy"),
               `source: orders is warehouse.table('compile_cache_orders')\n// ${n}\n`,
            );
         try {
            edit(0);
            await env.load();
            expect(env.ordersFields()).toBe('["id"]');
            expect(fetched).toHaveBeenCalledTimes(1);
            await env.warehouse.runSQL(
               "create or replace table compile_cache_orders as select 1 as id, 2 as added_col",
            );
            // Without a refresh the connection's cached schema answers.
            edit(1);
            await env.load();
            expect(env.ordersFields()).toBe('["id"]');
            expect(fetched).toHaveBeenCalledTimes(1);
            edit(2);
            refreshPackageConnectionsEpoch("publisher-env", "pkg");
            await env.load();
            expect(env.ordersFields()).toBe('["id","added_col"]');
            expect(fetched).toHaveBeenCalledTimes(2);
            // One-shot: later loads reuse what the refresh fetched.
            edit(3);
            await env.load();
            expect(env.ordersFields()).toBe('["id","added_col"]');
            expect(fetched).toHaveBeenCalledTimes(2);
         } finally {
            await env.cleanup();
         }
      });
   });

   describe("gate metrics", () => {
      it("records a model's load-time gate findings on every load, reused or not", async () => {
         fs.writeFileSync(
            path.join(packagePath, "gated.malloy"),
            `#(authorize) true
source: open_true is duckdb.sql("select 1 as x") extend { measure: c is count() }
`,
         );
         const harness = await startMetricsHarness();
         // Drop the cached instrument so it re-binds to this test's provider.
         resetAuthorizeGuardTelemetryForTesting();
         const outcomes = recordLoads();
         const { malloyConfig, duckdb } = duckdbMalloyConfig();
         const packages: Package[] = [];
         try {
            const admitAll = () =>
               harness.collectCounter("publisher_authorize_admit_all_total");
            packages.push(
               await Package.create(
                  "metrics-env",
                  "pkg",
                  packagePath,
                  malloyConfig,
               ),
            );
            expect(await admitAll()).toBe(1);
            packages.push(
               await Package.create(
                  "metrics-env",
                  "pkg",
                  packagePath,
                  malloyConfig,
               ),
            );
            const gated = outcomes[1].models.find(
               (m) => m.modelPath === "gated.malloy",
            );
            expect(gated?.derivationReused).toBe(true);
            expect(await admitAll()).toBe(2);
            // A package-scope /compile is a dry run, not a publish.
            await compileSource(
               {
                  environmentName: "metrics-env",
                  environmentPath: tempDir,
                  withPackageLock: (_name, fn) => fn(),
                  loadPackageLocked: async () => packages[1],
               },
               "pkg",
               "gated.malloy",
               undefined,
               false,
               undefined,
               "package",
            );
            expect(await admitAll()).toBe(2);
         } finally {
            dropPackageCompileCache("metrics-env", "pkg");
            for (const pkg of packages) {
               await pkg.getMalloyConfig().releaseConnections();
            }
            await duckdb.close();
            resetAuthorizeGuardTelemetryForTesting();
            await harness.shutdown();
         }
      });
   });

   describe("through the environment", () => {
      let environment: Environment;

      beforeEach(async () => {
         environment = await Environment.create("compile-env", tempDir, []);
      });

      afterEach(async () => {
         dropPackageCompileCache("compile-env", "pkg");
         await environment.closeAllConnections();
      });

      it("keeps the worker's cache across a reload and drops it on unload", async () => {
         const outcomes = recordLoads();
         await environment.getPackage("pkg");
         await environment.getPackage("pkg", true);
         await environment.unloadPackage("pkg");
         await environment.getPackage("pkg");
         expect(outcomes.map((o) => o.timings.compileCacheHits)).toEqual([
            0, 3, 0,
         ]);
      });

      describe("reload with ifChanged", () => {
         let controller: PackageController;
         const reload = (options: {
            ifChanged?: boolean;
            refreshSchema?: boolean;
         }) => controller.reloadPackage("compile-env", "pkg", "mcp", options);

         beforeEach(async () => {
            controller = new PackageController({
               getEnvironment: async () => environment,
            } as unknown as EnvironmentStore);
            await environment.getPackage("pkg");
         });

         it("loads nothing and reports no timings when the tree is unchanged", async () => {
            const outcomes = recordLoads();
            const served = (
               await environment.getPackage("pkg")
            ).getSourceContentSha();
            const result = await reload({ ifChanged: true });
            expect(result.unchanged).toBe(true);
            expect(result.timings).toBeUndefined();
            expect(result.metadata.sourceContentSha).toBe(served);
            expect(outcomes).toHaveLength(0);
         });

         it("reloads when a model file was added", async () => {
            const outcomes = recordLoads();
            fs.writeFileSync(
               path.join(packagePath, "added.malloy"),
               `source: added is duckdb.sql("select 3 as id")\n`,
            );
            const result = await reload({ ifChanged: true });
            expect(result.unchanged).toBeUndefined();
            expect(outcomes).toHaveLength(1);
            expect(result.metadata.sourceContentSha).toBe(
               (await environment.getPackage("pkg")).getSourceContentSha(),
            );
            expect(await reload({ ifChanged: true })).toMatchObject({
               unchanged: true,
            });
         });

         it("reloads when a model file was removed", async () => {
            const outcomes = recordLoads();
            fs.rmSync(path.join(packagePath, "other.malloy"));
            const result = await reload({ ifChanged: true });
            expect(result.unchanged).toBeUndefined();
            expect(outcomes).toHaveLength(1);
            expect(await reload({ ifChanged: true })).toMatchObject({
               unchanged: true,
            });
         });

         it("reloads when a model file changed, with the timings of that load", async () => {
            const outcomes = recordLoads();
            fs.writeFileSync(
               path.join(packagePath, "other.malloy"),
               `${OTHER}// edited\n`,
            );
            const result = await reload({ ifChanged: true });
            expect(result.unchanged).toBeUndefined();
            expect(outcomes).toHaveLength(1);
            expect(result.timings).toEqual({
               compileDurationMs: expect.any(Number),
               schemaFetchDurationMs: expect.any(Number),
               schemaFetchCount: expect.any(Number),
               schemaCacheHits: expect.any(Number),
               compileCacheHits: 2,
               totalMs: expect.any(Number),
            });
         });

         it("reloads for edits the receipt sha does not cover, and skips a rewrite with the same content", async () => {
            const csv = path.join(packagePath, "rows.csv");
            fs.writeFileSync(csv, "id\n1\n");
            fs.writeFileSync(
               path.join(packagePath, "rows.malloy"),
               `source: rows is duckdb.table('${csv.replace(/\\/g, "/")}')\n`,
            );
            await reload({});
            const outcomes = recordLoads();
            const edits: Array<[string, () => void]> = [
               [
                  "publisher.json",
                  () =>
                     fs.writeFileSync(
                        path.join(packagePath, "publisher.json"),
                        JSON.stringify({ name: "pkg", description: "edited" }),
                     ),
               ],
               [
                  "a CSV column",
                  () => fs.writeFileSync(csv, "id,added_column\n1,2\n"),
               ],
               [
                  "a styles file",
                  () =>
                     fs.writeFileSync(
                        path.join(packagePath, "base.styles.json"),
                        JSON.stringify({ by_id: { renderer: "bar" } }),
                     ),
               ],
            ];
            let loads = 0;
            for (const [, edit] of edits) {
               edit();
               expect(
                  (await reload({ ifChanged: true })).unchanged,
               ).toBeUndefined();
               loads += 1;
               expect(outcomes).toHaveLength(loads);
               expect((await reload({ ifChanged: true })).unchanged).toBe(true);
            }
            fs.writeFileSync(path.join(packagePath, "other.malloy"), OTHER);
            expect((await reload({ ifChanged: true })).unchanged).toBe(true);
            expect(outcomes).toHaveLength(loads);
         });

         it("reloads, and fails, for a publisher.json rewritten to []", async () => {
            fs.writeFileSync(path.join(packagePath, "publisher.json"), "[]");
            await expect(reload({ ifChanged: true })).rejects.toThrow();
         });

         it("reloads after a failed reload whose edit was reverted, until a load succeeds", async () => {
            fs.writeFileSync(
               path.join(packagePath, "other.malloy"),
               "source: broken is",
            );
            await expect(reload({})).rejects.toThrow();
            fs.writeFileSync(path.join(packagePath, "other.malloy"), OTHER);
            const outcomes = recordLoads();
            expect(
               (await reload({ ifChanged: true })).unchanged,
            ).toBeUndefined();
            expect(outcomes).toHaveLength(1);
            expect(environment.getStaleCompileErrors().size).toBe(0);
            expect((await reload({ ifChanged: true })).unchanged).toBe(true);
         });

         it("reloads next time when a write landed while the load was in flight", async () => {
            const loadPackage = workers.pool.loadPackage.bind(workers.pool);
            const spy = spyOn(workers.pool, "loadPackage").mockImplementation(
               async (request) => {
                  // After the fingerprint was taken, before the worker lists the tree.
                  fs.writeFileSync(
                     path.join(packagePath, "late.malloy"),
                     `source: late is duckdb.sql("select 4 as id")\n`,
                  );
                  return loadPackage(request);
               },
            );
            try {
               await reload({});
            } finally {
               spy.mockRestore();
            }
            const outcomes = recordLoads();
            expect(
               (await reload({ ifChanged: true })).unchanged,
            ).toBeUndefined();
            expect(outcomes).toHaveLength(1);
         });

         it("reloads an unchanged tree when refreshSchema is set, from cold caches", async () => {
            const outcomes = recordLoads();
            const refreshed = await reload({
               ifChanged: true,
               refreshSchema: true,
            });
            expect(refreshed.unchanged).toBeUndefined();
            expect(refreshed.timings?.compileCacheHits).toBe(0);
            // Only that load was cold: a plain reload is served by the cache again.
            const next = await reload({});
            expect(next.timings?.compileCacheHits).toBe(3);
            expect(outcomes).toHaveLength(2);
         });
      });

      it("recompiles a model whose data file changed under the same name", async () => {
         const csv = path.join(packagePath, "rows.csv");
         fs.writeFileSync(csv, "id\n1\n");
         fs.writeFileSync(
            path.join(packagePath, "rows.malloy"),
            `source: rows is duckdb.table('${csv.replace(/\\/g, "/")}')\n`,
         );
         const outcomes = recordLoads();
         await environment.getPackage("pkg");
         fs.writeFileSync(csv, "id,added_column\n1,2\n");
         await environment.getPackage("pkg", true);
         const reloaded = outcomes[1].models.find(
            (m) => m.modelPath === "rows.malloy",
         );
         expect(reloaded?.compileCacheHit).toBe(false);
         expect(JSON.stringify(reloaded?.modelDef)).toContain("added_column");
      });
   });
});
