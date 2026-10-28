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
} from "../../src/package_load/package_load_pool";
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
      it("keeps the cache across a token-only PATCH and empties it on a host change", async () => {
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
         const patch = (uri: string) =>
            service.updateConnection("publisher-env", "warehouse", {
               publisherConnection: { connectionUri: uri, accessToken: "new" },
            });
         const outcomes = recordLoads();
         const packages: Package[] = [];
         const load = async () => {
            packages.push(
               await Package.create("publisher-env", "pkg", packagePath, () =>
                  environment.getEnvironmentMalloyConfig(),
               ),
            );
            return outcomes[outcomes.length - 1].timings.compileCacheHits;
         };
         try {
            expect(await load()).toBe(0);
            await patch(remote.connectionUri("warehouse"));
            expect(await load()).toBe(4);
            await patch(moved.connectionUri("warehouse"));
            expect(await load()).toBe(0);
         } finally {
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
