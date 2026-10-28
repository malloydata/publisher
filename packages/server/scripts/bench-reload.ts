// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Reload benchmark: loads a synthetic DuckDB-backed package once, then reloads
 * it three ways through the production load path (`Package.create` over the
 * worker pool) and prints the worker's phase split for each.
 *
 *   bun run bench:reload [modelCount]      (default 73)
 *
 * Cases, each measured after the cold load:
 *   c  unchanged tree, same connection instances
 *   b  one leaf model edited, same connection instances
 *   a  token-only connection update: a new instance with the same name and
 *      digest, which is what a connection PATCH produces
 */
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { FixedConnectionMap, MalloyConfig } from "@malloydata/malloy";
import fs from "fs";
import os from "os";
import path from "path";
import { logger } from "../src/logger";
import {
   PackageLoadPool,
   __setPackageLoadPoolForTests,
} from "../src/package_load/package_load_pool";
import type { PackageLoadOutcome } from "../src/package_load/protocol";
import { Package } from "../src/service/package";

const BASE_COUNT = 3;
const ENV = "bench-env";

/** `warehouse` is not `duckdb`, so the worker fetches its schemas over the proxy. */
function warehouseConfig(connection: DuckDBConnection): MalloyConfig {
   const config = new MalloyConfig({ connections: {} });
   config.wrapConnections(
      () =>
         new FixedConnectionMap(
            new Map([["warehouse", connection]]),
            "warehouse",
         ),
   );
   return config;
}

async function newWarehouse(modelCount: number): Promise<DuckDBConnection> {
   const connection = new DuckDBConnection("warehouse", ":memory:");
   for (let i = 0; i < modelCount; i++) {
      await connection.runSQL(
         `create table if not exists t_${i} as select ${i} as id, 'n${i}' as name, ${i}.5 as amount`,
      );
   }
   return connection;
}

/** A few shared bases; every other model imports one and adds a source with 3 views. */
function writeModels(dir: string, modelCount: number, leafSuffix = ""): void {
   fs.writeFileSync(
      path.join(dir, "publisher.json"),
      JSON.stringify({ name: "bench" }),
   );
   for (let b = 0; b < BASE_COUNT; b++) {
      fs.writeFileSync(
         path.join(dir, `base_${b}.malloy`),
         `source: base_${b} is warehouse.table('t_${b}') extend {
  measure: total is amount.sum()
  dimension: label is name
}
`,
      );
   }
   for (let i = BASE_COUNT; i < modelCount; i++) {
      const b = i % BASE_COUNT;
      const edit = i === modelCount - 1 ? leafSuffix : "";
      fs.writeFileSync(
         path.join(dir, `model_${i}.malloy`),
         `import "base_${b}.malloy"
source: m_${i} is warehouse.table('t_${i}') extend {
  measure: total is amount.sum()
  view: by_name is { group_by: name aggregate: total }
  view: top_n is { group_by: id aggregate: total limit: 5 }
  view: overall is { aggregate: total, rows is count() }
}
source: joined_${i} is base_${b} extend {
  view: by_label is { group_by: label aggregate: total }
}
${edit}`,
      );
   }
}

async function main(): Promise<void> {
   const modelCount = Number(process.argv[2] ?? 73);
   if (!Number.isInteger(modelCount) || modelCount <= BASE_COUNT) {
      throw new Error(`modelCount must be an integer above ${BASE_COUNT}`);
   }
   process.env.PACKAGE_LOAD_WORKERS = "1";
   const pool = new PackageLoadPool(1);
   await __setPackageLoadPoolForTests(pool);

   const outcomes: PackageLoadOutcome[] = [];
   const loadPackage = pool.loadPackage.bind(pool);
   pool.loadPackage = async (request) => {
      const outcome = await loadPackage(request);
      outcomes.push(outcome);
      return outcome;
   };
   // Package.create logs per load; keep the table readable.
   logger.level = "error";

   const dir = fs.mkdtempSync(
      path.join(os.tmpdir(), "publisher-bench-reload-"),
   );
   const warehouse = await newWarehouse(modelCount);
   const tokenUpdated = await newWarehouse(modelCount);
   try {
      writeModels(dir, modelCount);
      const config = warehouseConfig(warehouse);

      const rows: Record<string, string | number>[] = [];
      const measure = async (label: string, malloyConfig: MalloyConfig) => {
         const start = performance.now();
         await Package.create(ENV, "bench", dir, malloyConfig);
         const wall = performance.now() - start;
         const { timings } = outcomes[outcomes.length - 1];
         rows.push({
            case: label,
            wallMs: Math.round(wall),
            compileDurationMs: Math.round(timings.compileDurationMs),
            schemaFetchDurationMs: Math.round(timings.schemaFetchDurationMs),
            schemaFetchCount: timings.schemaFetchCount,
            schemaCacheHits: timings.schemaCacheHits ?? 0,
         });
      };

      await measure("cold load", config);
      await measure("c: unchanged tree", config);
      writeModels(
         dir,
         modelCount,
         `source: edited is m_${modelCount - 1} extend {}\n`,
      );
      await measure("b: one leaf edited", config);
      await measure(
         "a: token-only connection update",
         warehouseConfig(tokenUpdated),
      );

      console.log(`models=${modelCount} (bases=${BASE_COUNT})`);
      console.table(rows);
   } finally {
      await warehouse.close();
      await tokenUpdated.close();
      fs.rmSync(dir, { recursive: true, force: true });
      await __setPackageLoadPoolForTests(null);
   }
}

main().then(
   () => process.exit(0),
   (error) => {
      console.error(error);
      process.exit(1);
   },
);
