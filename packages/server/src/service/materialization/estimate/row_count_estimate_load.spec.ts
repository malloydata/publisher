// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Row-count estimates over a real package on DuckDB: the counts are the rows a
// build would write, a rollup counts its grain, a gated or caller-scoped source
// is refused without a statement, and loading the package runs no count at all.
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as fsp from "fs/promises";
import * as os from "os";
import * as path from "path";

import { Environment } from "../../environment";
import { parsePackageName } from "../../identity";
import type { Package } from "../../package";
import { writePackageFiles } from "../../../test_helpers/temp_package";
import {
   COUNT_QUERY_METHOD,
   estimatePackageRowCounts,
   ROW_ESTIMATE_ALIAS,
} from "./row_count_estimate";

// Five orders over three categories and four days; two tenants.
const ROWS = `
  SELECT * FROM (VALUES
    (10, 'A', 't1', TIMESTAMP '2024-01-01 08:00:00'),
    (20, 'A', 't1', TIMESTAMP '2024-01-01 09:00:00'),
    (30, 'B', 't2', TIMESTAMP '2024-01-02 08:00:00'),
    (40, 'B', 't2', TIMESTAMP '2024-01-03 08:00:00'),
    (50, 'C', 't1', TIMESTAMP '2024-01-04 08:00:00')
  ) AS t(amount, category, tenant, order_time)`;

const MODEL = `##! experimental { persistence composite_sources givens }
source: orders is duckdb.sql("""${ROWS}""") extend {
  dimension: order_day is order_time.day
  #@ preaggregate grain="category"
  measure: total is amount.sum()
}

#@ persist
source: orders_by_day is orders -> {
  group_by: order_day, category
  aggregate: day_total is amount.sum()
}

#@ persist
source: order_lines is orders -> { select: amount, category, tenant }
`;

// A gate on the source a persisted query reads: the strictest gate refuses it.
// The rollup over it is refused by the plan itself.
const GATED = `##! experimental { persistence givens }
#(secure)
given:
  TENANTS :: string[]

#(access_filter) tenant in $TENANTS
source: secured is duckdb.sql("""${ROWS}""") extend {
  #@ preaggregate grain="category"
  measure: secured_total is amount.sum()
}

#@ persist
source: secured_by_category is secured -> {
  group_by: category, tenant
  aggregate: n is count()
}
`;

// A given in the extend block: left out of the build, re-applied per caller.
const SCOPED = `##! experimental { persistence givens }
given:
  TENANT :: string is "t1"

#@ persist
source: scoped_lines is duckdb.sql("""${ROWS}""") -> {
  select: amount, category, tenant
} extend {
  where: tenant = $TENANT
}
`;

let rootDir: string;
let envPath: string;
let statements: string[];
let restoreRunSQL: (() => void) | undefined;

beforeEach(async () => {
   rootDir = await fsp.mkdtemp(path.join(os.tmpdir(), "publisher-estimate-"));
   envPath = path.join(rootDir, "env");
   fs.mkdirSync(envPath, { recursive: true });
   // Every statement any in-process DuckDB runs, from before the load.
   statements = [];
   const original = DuckDBConnection.prototype.runSQL;
   DuckDBConnection.prototype.runSQL = function (
      this: DuckDBConnection,
      sql: string,
      ...rest: unknown[]
   ) {
      statements.push(sql);
      return (original as (...args: unknown[]) => unknown).call(
         this,
         sql,
         ...rest,
      );
   } as typeof original;
   restoreRunSQL = () => {
      DuckDBConnection.prototype.runSQL = original;
   };
});

afterEach(async () => {
   restoreRunSQL?.();
   await fsp.rm(rootDir, { recursive: true, force: true }).catch(() => {});
});

async function loadPackage(files: Record<string, string>): Promise<Package> {
   writePackageFiles(path.join(envPath, "pkg"), files, {
      manifest: { name: "pkg", description: "fixture" },
   });
   const env = await Environment.create("testEnv", envPath, []);
   await env.addPackage("pkg");
   return env.getPackage(parsePackageName("pkg"), false);
}

const estimateStatements = () =>
   statements.filter((sql) => sql.includes(ROW_ESTIMATE_ALIAS));

function bySourceName(
   estimates: Awaited<ReturnType<typeof estimatePackageRowCounts>>,
) {
   return Object.fromEntries(
      Object.values(estimates.estimates).map((e) => [e.name, e]),
   );
}

describe("row-count estimates over DuckDB", () => {
   it(
      "runs no count when the package loads or reloads, only when asked",
      async () => {
         const pkg = await loadPackage({ "model.malloy": MODEL });
         expect(Object.keys(pkg.getBuildPlan()?.sources ?? {})).toHaveLength(3);
         await pkg.reloadAllModels({});
         expect(estimateStatements()).toEqual([]);

         await estimatePackageRowCounts({
            pkg,
            plan: pkg.getBuildPlan() ?? { graphs: [], sources: {} },
            environmentName: "testEnv",
            body: {},
         });
         // One count per planned source, and so the spy does see them.
         expect(estimateStatements()).toHaveLength(3);
      },
      { timeout: 100000 },
   );

   it(
      "counts what each build would write, and a rollup's grain",
      async () => {
         const pkg = await loadPackage({ "model.malloy": MODEL });
         const result = await estimatePackageRowCounts({
            pkg,
            plan: pkg.getBuildPlan() ?? { graphs: [], sources: {} },
            environmentName: "testEnv",
            body: { maxRows: 4 },
         });
         const estimates = bySourceName(result);

         expect(estimates.orders_by_day).toMatchObject({
            status: "estimated",
            rows: 4,
            band: { low: 4, high: 4 },
            method: COUNT_QUERY_METHOD,
            origin: "persist",
            modelPath: "model.malloy",
            exceedsMaxRows: false,
         });
         expect(estimates.order_lines).toMatchObject({
            status: "estimated",
            rows: 5,
            exceedsMaxRows: true,
         });
         const rollup = Object.values(result.estimates).find(
            (e) => e.origin === "preaggregate",
         );
         expect(rollup).toMatchObject({
            status: "estimated",
            rows: 3,
            band: { low: 3, high: 3 },
            baseSourceName: "orders",
            grainDimensions: ["category"],
            exceedsMaxRows: false,
         });
         expect(result.maxRows).toBe(4);
      },
      { timeout: 100000 },
   );

   it(
      "estimates only the sources a request names",
      async () => {
         const pkg = await loadPackage({ "model.malloy": MODEL });
         const result = await estimatePackageRowCounts({
            pkg,
            plan: pkg.getBuildPlan() ?? { graphs: [], sources: {} },
            environmentName: "testEnv",
            body: { sources: ["order_lines"] },
         });
         expect(Object.values(result.estimates).map((e) => e.name)).toEqual([
            "order_lines",
         ]);
         expect(estimateStatements()).toHaveLength(1);
      },
      { timeout: 100000 },
   );

   it(
      "refuses a gated source and a caller-scoped one without a statement",
      async () => {
         const pkg = await loadPackage({
            "gated.malloy": GATED,
            "scoped.malloy": SCOPED,
         });
         const result = await estimatePackageRowCounts({
            pkg,
            plan: pkg.getBuildPlan() ?? { graphs: [], sources: {} },
            environmentName: "testEnv",
            body: {},
         });
         const estimates = bySourceName(result);

         const gated = estimates.secured_by_category;
         expect(gated.status).toBe("refused");
         expect(gated.reason).toBe("gated");
         expect(gated.rows).toBeUndefined();

         const rollup = Object.values(result.estimates).find(
            (e) => e.origin === "preaggregate",
         );
         // The plan refuses it (authorize) but keeps its entry: its table
         // exists and is gated.
         expect(rollup).toMatchObject({
            status: "refused",
            reason: "gated",
            baseSourceName: "secured",
         });
         expect(rollup?.rows).toBeUndefined();

         expect(estimates.scoped_lines).toMatchObject({
            status: "refused",
            reason: "caller_scoped",
         });
         expect(estimates.scoped_lines.rows).toBeUndefined();

         expect(estimateStatements()).toEqual([]);
      },
      { timeout: 100000 },
   );
});

describe("who can run an estimate", () => {
   it("is only the estimate route's controller: no load, reload or build path imports it", () => {
      const srcRoot = path.resolve(import.meta.dir, "../../..");
      const importers: string[] = [];
      const walk = (dir: string) => {
         for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
            const full = path.join(dir, entry.name);
            if (entry.isDirectory()) walk(full);
            else if (
               entry.name.endsWith(".ts") &&
               !entry.name.endsWith(".spec.ts") &&
               !full.endsWith(path.join("estimate", "row_count_estimate.ts")) &&
               /from\s+"[^"]*\/row_count_estimate"/.test(
                  fs.readFileSync(full, "utf8"),
               )
            ) {
               importers.push(path.relative(srcRoot, full));
            }
         }
      };
      walk(srcRoot);
      expect(importers).toEqual([
         path.join("controller", "package.controller.ts"),
      ]);
   });
});
