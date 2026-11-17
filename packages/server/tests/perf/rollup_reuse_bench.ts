// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What a database rollup saves by building from its base's stored table: a
 * standalone run of a base and its rollup over a synthetic DuckDB table of
 * ROWS rows, counting the run's statements that scan the raw rows and timing
 * the rollup's build, then timing the same rollup built from the warehouse
 * (its planned SQL) on the same connection. Two bases: a `#@ persist` one
 * grouped to 50k rows with a `#@ preaggregate` rollup, and a `#(materialize)`
 * one that stores the raw rows with a computed dimension, with a
 * `#(preaggregate)` rollup at that dimension.
 *
 *   bun tests/perf/rollup_reuse_bench.ts [rows]
 */

import { DuckDBConnection as MalloyDuckDBConnection } from "@malloydata/db-duckdb";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { Environment } from "../../src/service/environment";
import type { EnvironmentStore } from "../../src/service/environment_store";
import { parsePackageName } from "../../src/service/identity";
import { MaterializationService } from "../../src/service/materialization/run/materialization_service";
import { DuckDBConnection } from "../../src/storage/duckdb/DuckDBConnection";
import { createDuckDBRepository } from "../../src/storage/duckdb/DuckDBRepository";
import { initializeSchema } from "../../src/storage/duckdb/schema";
import { writePackageFiles } from "../../src/test_helpers/temp_package";

const ROWS = Number(process.argv[2] ?? 5_000_000);
const MARK = "bench_raw_rows";

const RAW = `source: raw is duckdb.sql("""
  SELECT i % 50 AS cat, i % 1000 AS sub, (i % 97) * 1.5 AS amt, '${MARK}' AS mark
  FROM range(${ROWS}) AS t(i)
""")`;

const PERSIST_MODEL = `##! experimental.persistence
${RAW}

#@ persist
source: daily is raw -> {
  group_by: cat, sub
  aggregate: total is amt.sum(), n is count()
} extend {
  #@ preaggregate grain="cat"
  measure: grand is total.sum()
  #@ preaggregate grain="cat"
  measure: rows is n.sum()
}
`;

const MATERIALIZE_MODEL = `##! experimental.persistence
${RAW}

#(materialize) name="bench_orders"
source: orders is raw extend {
  dimension: band is concat('band-', cast(cat % 10 as string))
  #(preaggregate) grain=["band"]
  measure: grand is amt.sum()
  #(preaggregate) grain=["band"]
  measure: rows is count()
}
`;

async function bench(base: string, model: string): Promise<unknown> {
   const root = fs.realpathSync(
      fs.mkdtempSync(path.join(os.tmpdir(), "rollup-reuse-bench-")),
   );
   const envPath = path.join(root, "env");
   fs.mkdirSync(envPath);
   writePackageFiles(
      path.join(envPath, "pkg"),
      { "m.malloy": model },
      { manifest: { name: "pkg", description: "bench" } },
   );
   const db = new DuckDBConnection(":memory:");
   await db.initialize();
   await initializeSchema(db);
   await db.run(
      "INSERT INTO environments (id, name, path, created_at, updated_at) VALUES (?, ?, ?, now(), now())",
      ["env-1", "e1", envPath],
   );
   const repo = createDuckDBRepository(db);
   const env = await Environment.create("e1", envPath, []);
   await env.addPackage("pkg");
   const service = new MaterializationService({
      storageManager: { getRepository: () => repo },
      getEnvironment: async () => env,
      getLoadedEnvironments: () => [env],
   } as unknown as EnvironmentStore);

   const statements: { sql: string; ms: number }[] = [];
   const runSQL = MalloyDuckDBConnection.prototype.runSQL;
   MalloyDuckDBConnection.prototype.runSQL = async function (
      this: MalloyDuckDBConnection,
      sql: string,
      ...rest: unknown[]
   ) {
      const started = performance.now();
      try {
         return await (runSQL as (...a: unknown[]) => Promise<never>).call(
            this,
            sql,
            ...rest,
         );
      } finally {
         statements.push({ sql, ms: performance.now() - started });
      }
   } as never;

   const created = await service.createMaterialization("e1", "pkg", {});
   let m;
   for (;;) {
      m = await repo.getMaterializationById(created.id);
      if (m && ["MANIFEST_FILE_READY", "FAILED"].includes(m.status)) break;
      await new Promise((resolve) => setTimeout(resolve, 20));
   }
   if (m.status !== "MANIFEST_FILE_READY") throw new Error(String(m.error));
   const entries = Object.values(
      (
         m.manifest as unknown as {
            entries: Record<string, Record<string, string>>;
         }
      ).entries,
   );
   const rollup = entries.find((e) => e.origin === "preaggregate")!;
   const creates = statements.filter((s) => /^\s*CREATE\b/i.test(s.sql));
   const rawScans = creates.filter((s) => s.sql.includes(MARK)).length;
   const reused = creates.find((s) =>
      s.sql.includes(rollup.physicalTableName),
   )!;

   const pkg = await env.getPackage(parsePackageName("pkg"), false);
   const planned = Object.values(pkg.getBuildPlan()!.sources).find(
      (s) => s.sourceEntityId === rollup.sourceEntityId,
   )!;
   const connection = (await pkg.getMalloyConnection(
      "duckdb",
   )) as MalloyDuckDBConnection;
   const started = performance.now();
   await runSQL.call(
      connection,
      `CREATE TEMP TABLE bench_warehouse_rollup AS ${planned.sql}`,
   );
   const warehouseMs = performance.now() - started;

   const result = {
      base,
      rows: ROWS,
      tablesBuilt: creates.length,
      buildStatementsScanningRawRows: rawScans,
      rollupUpstreamReuse: rollup.upstreamReuse ?? null,
      rollupBuildReadsBaseTable: !reused.sql.includes(MARK),
      rollupBuildMsFromBaseTable: Math.round(reused.ms),
      rollupBuildMsFromWarehouse: Math.round(warehouseMs),
   };
   MalloyDuckDBConnection.prototype.runSQL = runSQL;
   await env.closeAllConnections().catch(() => undefined);
   await db.close();
   fs.rmSync(root, { recursive: true, force: true });
   return result;
}

async function main(): Promise<void> {
   const results = [
      await bench("#@ persist", PERSIST_MODEL),
      await bench("#(materialize)", MATERIALIZE_MODEL),
   ];
   console.log(JSON.stringify(results, null, 2));
}

main().then(
   () => process.exit(0),
   (err) => {
      console.error(err);
      process.exit(1);
   },
);
