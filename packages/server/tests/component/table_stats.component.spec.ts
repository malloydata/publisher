// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Table stats end to end (materialization v2, T2): a real package, a real
 * colocated build run by the real MaterializationService into a real run store,
 * real queries through the one record-emitting wrapper into the workload log's
 * local store, and the stats read back from both.
 */

import { afterAll, beforeAll, describe, expect, it, spyOn } from "bun:test";
import * as fs from "fs/promises";
import * as os from "os";
import * as path from "path";
import { logger } from "../../src/logger";
import { withAccessRecord } from "../../src/service/access_record";
import { Environment } from "../../src/service/environment";
import { parsePackageName } from "../../src/service/identity";
import type { MaterializationHost } from "../../src/service/materialization/host";
import { MaterializationService } from "../../src/service/materialization/run/materialization_service";
import { QueryPrivileges } from "../../src/service/model_query_request";
import type { Package } from "../../src/service/package";
import {
   setWorkloadLogSink,
   WorkloadLogStore,
} from "../../src/service/workload_log_store";
import { createDuckDBRepository } from "../../src/storage/duckdb/DuckDBRepository";
import { DuckDBConnection } from "../../src/storage/duckdb/DuckDBConnection";
import { initializeSchema } from "../../src/storage/duckdb/schema";
import { writePackageFiles } from "../../src/test_helpers/temp_package";

const MODEL = `##! experimental.persistence
source: orders is duckdb.sql("""
  SELECT * FROM (VALUES
    (1, DATE '2026-01-01', 'east', 100.5),
    (2, DATE '2026-01-02', 'west', 3.5),
    (3, DATE '2026-01-02', 'east', 7.0)
  ) AS t(id, order_date, region, amount)
""") extend { primary_key: id }

#@ persist name="daily"
source: daily is orders -> {
  group_by: order_date, region
  aggregate: total is amount.sum(), order_count is count()
}

#@ persist name="by_region"
source: by_region is orders -> { group_by: region; aggregate: n is count() }
`;

const TERMINAL = new Set(["MANIFEST_FILE_READY", "FAILED", "CANCELLED"]);

describe("table stats from a real build and real queries", () => {
   let rootDir: string;
   let db: DuckDBConnection;
   let store: WorkloadLogStore;
   let service: MaterializationService;
   let pkg: Package;
   let info: ReturnType<typeof spyOn>;

   beforeAll(async () => {
      info = spyOn(logger, "info").mockImplementation(() => logger);
      rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "publisher-stats-"));
      const envPath = path.join(rootDir, "env");
      await fs.mkdir(envPath, { recursive: true });
      writePackageFiles(
         path.join(envPath, "pkg"),
         { "model.malloy": MODEL },
         { manifest: { name: "pkg", description: "fixture" } },
      );
      const env = await Environment.create("testEnv", envPath, []);
      await env.addPackage("pkg");
      pkg = await env.getPackage(parsePackageName("pkg"), false);

      db = new DuckDBConnection(":memory:");
      await db.initialize();
      await initializeSchema(db, false);
      const repository = createDuckDBRepository(db);
      await repository.createEnvironment({ name: "testEnv", path: envPath });
      const host = {
         storageManager: { getRepository: () => repository },
         getEnvironment: async () => env,
         getLoadedEnvironments: () => [env],
      } as unknown as MaterializationHost;
      service = new MaterializationService(host);

      store = new WorkloadLogStore(
         { isInitialized: () => true, getDuckDbConnection: () => db },
         { retentionDays: 30, maxRows: 1000 },
      );
      setWorkloadLogSink(store);
   });

   afterAll(async () => {
      setWorkloadLogSink(undefined);
      info.mockRestore();
      await db.close?.();
      await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
   });

   function entityIdOf(sourceName: string): string {
      const source = Object.values(pkg.getBuildPlan()?.sources ?? {}).find(
         (s) => s.name === sourceName,
      );
      if (!source) throw new Error(`${sourceName} is not in the build plan`);
      return source.sourceEntityId;
   }

   async function run(query: string): Promise<void> {
      const model = pkg.getModel("model.malloy");
      if (!model) throw new Error("model.malloy did not load");
      await withAccessRecord(
         {
            environment: "testEnv",
            package: "pkg",
            modelPath: "model.malloy",
            adHoc: true,
         },
         undefined,
         (audit) =>
            model.runQuery(
               audit,
               { query, filterParams: {} },
               QueryPrivileges.of({}),
            ),
      );
   }

   it(
      "reports each table's build, columns and use",
      async () => {
         // The store holds nothing yet: every table reads zero use, not unknown.
         const before = await service.tableStats("testEnv", "pkg");
         expect(before.workload).toMatchObject({
            available: true,
            answeredQueries: 0,
         });
         expect(before.tables.map((t) => [t.sourceName, t.builds])).toEqual([
            ["by_region", { succeeded: 0, failed: 0 }],
            ["daily", { succeeded: 0, failed: 0 }],
         ]);

         const created = await service.createMaterialization("testEnv", "pkg");
         let runState = created;
         for (let i = 0; i < 200 && !TERMINAL.has(runState.status); i++) {
            await new Promise((resolve) => setTimeout(resolve, 50));
            runState = await service.getMaterialization(
               "testEnv",
               "pkg",
               created.id,
            );
         }
         expect(runState.status).toBe("MANIFEST_FILE_READY");

         // Two answered by `daily`'s table, one by `by_region`'s.
         await run(
            "run: daily -> { group_by: region; aggregate: t is total.sum() }",
         );
         await run("run: daily -> { aggregate: n is order_count.sum() }");
         await run(
            "run: by_region -> { group_by: region; aggregate: n.sum() }",
         );
         await store.stop();

         const stats = await service.tableStats("testEnv", "pkg");
         expect(stats.workload).toEqual({
            available: true,
            answeredQueries: 3,
            reportedQueries: 3,
         });
         expect(stats.runsRead).toBe(1);
         const daily = stats.tables.find((t) => t.sourceName === "daily");
         const byRegion = stats.tables.find(
            (t) => t.sourceName === "by_region",
         );
         expect(daily?.tableId).toBe(entityIdOf("daily"));
         expect(daily?.origin).toBe("persist");
         expect(daily?.builds.succeeded).toBe(1);
         expect(daily?.lastBuild?.materializationId).toBe(created.id);
         expect(daily?.lastBuild?.buildDurationMs).toBeGreaterThanOrEqual(0);
         // A colocated build does not count its rows (that scans the
         // warehouse), so its row count is unknown rather than a guess.
         expect(daily?.lastBuild?.rowCount).toBeUndefined();
         expect(daily?.columnsFrom).toBe("declared");
         expect(daily?.columns.map((c) => c.name)).toEqual([
            "order_date",
            "region",
            "total",
            "order_count",
         ]);
         expect(daily?.use).toEqual({ considered: 2, hits: 2, skips: {} });
         expect(byRegion?.builds.succeeded).toBe(1);
         expect(byRegion?.use).toEqual({ considered: 1, hits: 1, skips: {} });
      },
      { timeout: 60000 },
   );

   it("says use is unavailable, not zero, when the store is off", async () => {
      setWorkloadLogSink(undefined);
      const stats = await service.tableStats("testEnv", "pkg");
      expect(stats.workload.available).toBe(false);
      expect(stats.workload.unavailableReason).toContain(
         "PUBLISHER_WORKLOAD_LOG_STORE",
      );
      expect(stats.tables.every((t) => t.use === undefined)).toBe(true);
      // Builds are still read from the run store.
      expect(stats.tables.every((t) => t.builds.succeeded === 1)).toBe(true);
   });
});
