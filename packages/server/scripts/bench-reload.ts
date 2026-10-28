// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Reload benchmark: loads a synthetic DuckDB-backed package once (cold, which
 * also warms the worker), then reloads it three ways through the production
 * load path (`Package.create` over the worker pool) and prints the median of
 * REPS runs of the worker's phase split for each.
 *
 *   bun run bench:reload [modelCount] [--light]      (default 73, heavy shape)
 *
 * Cases:
 *   c  unchanged tree, same connection instances
 *   b  one shared base model edited, same connection instances
 *   a  token-only connection update: a new instance with the same name and
 *      digest, which is what a connection PATCH produces
 *
 * `--heavy` (default) gives every model enough fields and views to cost real
 * compile time; `--light` is the tiny shape.
 */
import { DuckDBConnection } from "@malloydata/db-duckdb";
import type { MalloyConfig } from "@malloydata/malloy";
import fs from "fs";
import path from "path";
import { logger } from "../src/logger";
import {
   PackageLoadPool,
   __setPackageLoadPoolForTests,
} from "../src/package_load/package_load_pool";
import type { PackageLoadOutcome } from "../src/package_load/protocol";
import { Package } from "../src/service/package";
import {
   duckdbMalloyConfig,
   writeTempPackage,
} from "../src/test_helpers/temp_package";

const BASE_COUNT = 3;
const ENV = "bench-env";
const REPS = 5;

interface Shape {
   name: string;
   baseFields: number;
   leafFields: number;
   views: number;
}
const SHAPES: Record<"heavy" | "light", Shape> = {
   heavy: { name: "heavy", baseFields: 20, leafFields: 40, views: 120 },
   light: { name: "light", baseFields: 2, leafFields: 2, views: 3 },
};

const cols = (prefix: string, n: number) =>
   Array.from({ length: n }, (_, k) => `${prefix}${k}`);

/** A `warehouse` table per model; the worker fetches their schemas over the proxy. */
async function newWarehouse(
   modelCount: number,
   shape: Shape,
): Promise<DuckDBConnection> {
   const connection = new DuckDBConnection("warehouse", ":memory:");
   const dims = cols("d", shape.leafFields);
   const vals = cols("v", shape.leafFields);
   const baseDims = cols("d", shape.baseFields);
   const baseVals = cols("v", shape.baseFields);
   const select = (id: number, d: string[], v: string[], extra: string) =>
      `select ${id} as id, ${extra}${d.map((c, k) => `'${c}_${id}' as ${c}`).join(", ")}, ${v.map((c, k) => `${id + k}.5 as ${c}`).join(", ")}`;
   for (let i = 0; i < BASE_COUNT; i++) {
      await connection.runSQL(
         `create table if not exists b_${i} as ${select(i, baseDims, baseVals, "")}`,
      );
   }
   for (let i = BASE_COUNT; i < modelCount; i++) {
      await connection.runSQL(
         `create table if not exists t_${i} as ${select(i, dims, vals, `${i % BASE_COUNT} as base_id, `)}`,
      );
   }
   return connection;
}

function baseModel(b: number, shape: Shape, edit = 0): string {
   const dims = cols("d", shape.baseFields);
   const vals = cols("v", shape.baseFields);
   return `source: base_${b} is warehouse.table('b_${b}') extend {
  primary_key: id
${dims.map((d) => `  dimension: ${d}_u is upper(${d})`).join("\n")}
${vals.map((v) => `  measure: ${v}_sum is ${v}.sum()`).join("\n")}
${edit ? `  measure: edit_${edit} is count()\n` : ""}}
`;
}

/** Each leaf imports one base, joins it, and defines fields plus grouped and nested views. */
function leafModel(i: number, shape: Shape): string {
   const b = i % BASE_COUNT;
   const dims = cols("d", shape.leafFields);
   const vals = cols("v", shape.leafFields);
   const views = Array.from({ length: shape.views }, (_, k) => {
      const g = dims[k % dims.length];
      const m = `${vals[k % vals.length]}_sum`;
      const nested =
         k % 2 === 1
            ? `\n    nest: sub is { group_by: ${dims[(k + 1) % dims.length]}_l aggregate: ${m} limit: 3 }`
            : "";
      return `  view: view_${k} is {
    group_by: ${g}
    bd is jb.${cols("d", shape.baseFields)[k % shape.baseFields]}_u
    aggregate: ${m}
    bm is jb.${cols("v", shape.baseFields)[k % shape.baseFields]}_sum
    rows is count()${nested}
    order_by: ${m} desc
    limit: 10
  }`;
   });
   return `import "base_${b}.malloy"
source: m_${i} is warehouse.table('t_${i}') extend {
  join_one: jb is base_${b} on jb.id = base_id
${dims.map((d) => `  dimension: ${d}_l is lower(${d})`).join("\n")}
${vals.map((v) => `  measure: ${v}_sum is ${v}.sum()`).join("\n")}
${views.join("\n")}
}
`;
}

function modelFiles(
   modelCount: number,
   shape: Shape,
   baseEdit = 0,
): Record<string, string> {
   const files: Record<string, string> = {};
   for (let b = 0; b < BASE_COUNT; b++) {
      files[`base_${b}.malloy`] = baseModel(b, shape, b === 0 ? baseEdit : 0);
   }
   for (let i = BASE_COUNT; i < modelCount; i++) {
      files[`model_${i}.malloy`] = leafModel(i, shape);
   }
   return files;
}

const median = (xs: number[]) => [...xs].sort((a, b) => a - b)[xs.length >> 1];

async function main(): Promise<void> {
   const args = process.argv.slice(2);
   const shape = args.includes("--light") ? SHAPES.light : SHAPES.heavy;
   const count = args.find((a) => !a.startsWith("--"));
   const modelCount = Number(count ?? 73);
   if (!Number.isInteger(modelCount) || modelCount <= BASE_COUNT) {
      throw new Error(`modelCount must be an integer above ${BASE_COUNT}`);
   }
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

   const warehouses: DuckDBConnection[] = [];
   const packages: Package[] = [];
   let cleanupDir = () => {};
   try {
      const { dir, cleanup } = writeTempPackage(modelFiles(modelCount, shape), {
         name: "bench",
         prefix: "publisher-bench-reload-",
      });
      cleanupDir = cleanup;
      const connect = async () => {
         const warehouse = await newWarehouse(modelCount, shape);
         warehouses.push(warehouse);
         return duckdbMalloyConfig(warehouse, "warehouse").malloyConfig;
      };
      const config = await connect();

      const load = async (malloyConfig: MalloyConfig) => {
         const start = performance.now();
         packages.push(await Package.create(ENV, "bench", dir, malloyConfig));
         const wall = performance.now() - start;
         return { wall, timings: outcomes[outcomes.length - 1].timings };
      };
      const summarize = async (
         label: string,
         reps: number,
         run: (rep: number) => ReturnType<typeof load>,
      ) => {
         const samples = [];
         for (let r = 0; r < reps; r++) samples.push(await run(r));
         const med = (pick: (s: (typeof samples)[number]) => number) =>
            Math.round(median(samples.map(pick)));
         return {
            case: label,
            wallMs: med((s) => s.wall),
            compileDurationMs: med((s) => s.timings.compileDurationMs),
            schemaFetchDurationMs: med((s) => s.timings.schemaFetchDurationMs),
            schemaFetchCount: med((s) => s.timings.schemaFetchCount),
            schemaCacheHits: med((s) => s.timings.schemaCacheHits ?? 0),
         };
      };

      const rows = [
         await summarize("cold load (warm-up)", 1, () => load(config)),
         await summarize("c: unchanged tree", REPS, () => load(config)),
         await summarize("b: one shared base edited", REPS, async (rep) => {
            fs.writeFileSync(
               path.join(dir, "base_0.malloy"),
               baseModel(0, shape, rep + 1),
            );
            return load(config);
         }),
         await summarize("a: token-only connection update", REPS, async () =>
            load(await connect()),
         ),
      ];
      console.log(
         `models=${modelCount} shape=${shape.name} (bases=${BASE_COUNT}, ` +
            `base fields=${shape.baseFields * 2 + 1}, leaf fields=${shape.leafFields * 2}, ` +
            `views/leaf=${shape.views}, one join per leaf); median of ${REPS} reps`,
      );
      console.table(rows);
   } finally {
      for (const pkg of packages)
         await pkg.getMalloyConfig().releaseConnections();
      for (const warehouse of warehouses) await warehouse.close();
      cleanupDir();
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
