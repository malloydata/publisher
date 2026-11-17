// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What a `#(materialize)` extension saves by building over its parent's
 * table when the parent is declared in another file (P10 across files):
 * `child` in `b.malloy` over `parent` in `a.malloy`, over a synthetic DuckDB
 * table of ROWS rows. Counts the run's statements that scan the raw rows and
 * times the child's build, then times the child built from the warehouse
 * (its planned SQL) on the same connection. With `rollup`, the same for a
 * colocated `#@ persist` base and its `#@ preaggregate` rollup
 * (rollup_reuse_bench.ts has more).
 *
 *   bun tests/perf/chain_reuse_bench.ts [rows] [rollup]
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
import { installSettings } from "../../src/test_helpers/settings_env";

const ROWS = Number(process.argv[2] ?? 5_000_000);
const CHAIN = process.argv[3] !== "rollup";
const MARK = "bench_raw_rows";
if (CHAIN) installSettings({ env: { PUBLISHER_MATERIALIZE: "true" } });

const MODEL = `##! experimental.persistence
source: raw is duckdb.sql("""
  SELECT i % 50 AS cat, i % 1000 AS sub, (i % 97) * 1.5 AS amt, '${MARK}' AS mark
  FROM range(${ROWS}) AS t(i)
""")

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

/** A two-file `#(materialize)` chain over the same rows. */
const CHAIN_FILES = {
   "a.malloy": `source: raw is duckdb.sql("""
  SELECT i % 50 AS cat, i % 1000 AS sub, (i % 97) * 1.5 AS amt, '${MARK}' AS mark
  FROM range(${ROWS}) AS t(i)
""")

#(materialize) name="parent_t"
source: parent is raw -> {
  group_by: cat, sub
  aggregate: total is amt.sum(), n is count()
}
`,
   "b.malloy": `import "a.malloy"

#(materialize) name="child_t"
source: child is parent extend {
  where: cat < 25
  dimension: label is concat(cat::string, '-', sub::string)
}
`,
};

async function main(): Promise<void> {
   const root = fs.realpathSync(
      fs.mkdtempSync(path.join(os.tmpdir(), "rollup-reuse-bench-")),
   );
   const envPath = path.join(root, "env");
   fs.mkdirSync(envPath);
   writePackageFiles(
      path.join(envPath, "pkg"),
      CHAIN ? CHAIN_FILES : { "m.malloy": MODEL },
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
   const rollup = entries.find((e) =>
      CHAIN ? e.sourceName === "child" : e.origin === "preaggregate",
   )!;
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

   console.log(
      JSON.stringify(
         {
            rows: ROWS,
            tablesBuilt: creates.length,
            buildStatementsScanningRawRows: rawScans,
            table: CHAIN ? "child (cross-file extension)" : "rollup",
            upstreamReuse: rollup.upstreamReuse ?? null,
            buildReadsUpstreamTable: !reused.sql.includes(MARK),
            buildMsFromUpstreamTable: Math.round(reused.ms),
            buildMsFromWarehouse: Math.round(warehouseMs),
         },
         null,
         2,
      ),
   );
   MalloyDuckDBConnection.prototype.runSQL = runSQL;
   await env.closeAllConnections().catch(() => undefined);
   await db.close();
   fs.rmSync(root, { recursive: true, force: true });
}

main().then(
   () => process.exit(0),
   (err) => {
      console.error(err);
      process.exit(1);
   },
);
