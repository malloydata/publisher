// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// The path report (materialization v2, D1) over a real compiled package with
// a real bound colocated manifest: which table answered, which could not and
// why, and the nearby tables the query could not use. The `storage=` tier is
// covered over the R4 harness in r4/path_report.component.spec.ts.
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs/promises";
import * as os from "os";
import * as path from "path";
import { unrecordedAudit } from "../../src/service/access_record";
import { Environment } from "../../src/service/environment";
import { parsePackageName } from "../../src/service/identity";
import type { Package } from "../../src/service/package";
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

source: daily_ext is daily extend { dimension: big_day is total > 100 }

#@ persist name="by_region"
source: by_region is orders -> { group_by: region; aggregate: n is count() }
`;

let rootDir: string;
let envPath: string;

beforeEach(async () => {
   rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "publisher-query-path-"));
   envPath = path.join(rootDir, "env");
   await fs.mkdir(envPath, { recursive: true });
});

afterEach(async () => {
   await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
});

async function loadPackage(model = MODEL): Promise<Package> {
   writePackageFiles(
      path.join(envPath, "pkg"),
      { "model.malloy": model },
      { manifest: { name: "pkg", description: "fixture" } },
   );
   const env = await Environment.create("testEnv", envPath, []);
   await env.addPackage("pkg");
   return env.getPackage(parsePackageName("pkg"), false);
}

function entityIdOf(pkg: Package, sourceName: string): string {
   const source = Object.values(pkg.getBuildPlan()?.sources ?? {}).find(
      (s) => s.name === sourceName,
   );
   if (!source) throw new Error(`${sourceName} is not in the build plan`);
   return source.sourceEntityId;
}

/** Build `sourceName`'s table from its plan SQL and bind it, with `extra` fields. */
async function buildAndBind(
   pkg: Package,
   entries: Record<string, Record<string, unknown>>,
): Promise<void> {
   const connection = (await pkg.getMalloyConnection(
      "duckdb",
   )) as DuckDBConnection;
   const bound: Record<string, Record<string, unknown>> = {};
   for (const [sourceName, extra] of Object.entries(entries)) {
      const source = Object.values(pkg.getBuildPlan()?.sources ?? {}).find(
         (s) => s.name === sourceName,
      );
      if (!source) throw new Error(`${sourceName} is not in the build plan`);
      const table = `built_${sourceName}`;
      await connection.runSQL(
         `CREATE OR REPLACE TABLE ${table} AS ${source.sql}`,
      );
      bound[source.sourceEntityId] = {
         tableName: table,
         connectionName: "duckdb",
         ...extra,
      };
   }
   pkg.bindColocatedServeManifest(bound as never);
}

async function pathOf(pkg: Package, query: string) {
   const model = pkg.getModel("model.malloy");
   if (!model) throw new Error("model.malloy did not load");
   const { path } = await model.runQuery(unrecordedAudit(), { query });
   if (!path) throw new Error("no path report");
   return path;
}

describe("the path report over colocated #@ persist", () => {
   it("reports an unbuilt table as NOT_BUILT and answers live", async () => {
      const pkg = await loadPackage();
      const report = await pathOf(
         pkg,
         "run: daily -> { group_by: region; aggregate: t is total.sum() }",
      );
      expect(report.servedBy).toEqual({ kind: "live" });
      expect(report.considered).toEqual([
         {
            tableId: entityIdOf(pkg, "daily"),
            kind: "materialized",
            outcome: "skipped",
            reason: "NOT_BUILT",
         },
      ]);
      expect(report.query).toEqual({
         sourceName: "daily",
         groupedBy: ["region"],
         filteredBy: [],
         measures: ["total"],
      });
   });

   it("names the bound table that answered, with its data-as-of", async () => {
      const pkg = await loadPackage();
      await buildAndBind(pkg, {
         daily: { dataAsOf: "2026-11-01T00:00:00.000Z" },
      });
      const report = await pathOf(
         pkg,
         "run: daily -> { group_by: region; aggregate: t is total.sum() }",
      );
      expect(report.servedBy).toEqual({
         kind: "materialized",
         tableId: entityIdOf(pkg, "daily"),
         dataAsOf: "2026-11-01T00:00:00.000Z",
      });
      expect(report.considered).toEqual([
         {
            tableId: entityIdOf(pkg, "daily"),
            kind: "materialized",
            outcome: "used",
         },
      ]);
   });

   it("answers an extension from the table it inherits", async () => {
      const pkg = await loadPackage();
      await buildAndBind(pkg, { daily: {} });
      const report = await pathOf(
         pkg,
         "run: daily_ext -> { where: big_day; group_by: region; aggregate: t is total.sum() }",
      );
      expect(report.servedBy).toMatchObject({
         kind: "materialized",
         tableId: entityIdOf(pkg, "daily"),
      });
      // Persistence is inherited, and an extension that adds only a dimension
      // builds the same SQL, so it is the same table, named once.
      expect(entityIdOf(pkg, "daily_ext")).toBe(entityIdOf(pkg, "daily"));
      expect(report.considered).toEqual([
         {
            tableId: entityIdOf(pkg, "daily"),
            kind: "materialized",
            outcome: "used",
         },
      ]);
      expect(report.query.filteredBy).toEqual(["big_day"]);
   });

   it("reports a table past its freshness window as STALE", async () => {
      const pkg = await loadPackage();
      await buildAndBind(pkg, {
         daily: {
            dataAsOf: "2020-01-01T00:00:00.000Z",
            freshnessWindowSeconds: 60,
            freshnessFallback: "live",
         },
      });
      const report = await pathOf(
         pkg,
         "run: daily -> { group_by: region; aggregate: t is total.sum() }",
      );
      expect(report.servedBy).toEqual({ kind: "live" });
      expect(report.considered).toEqual([
         {
            tableId: entityIdOf(pkg, "daily"),
            kind: "materialized",
            outcome: "skipped",
            reason: "STALE",
         },
      ]);
   });

   it("lists the tables on sources that extend the queried one, nearest first", async () => {
      const pkg = await loadPackage();
      await buildAndBind(pkg, { by_region: {} });
      const report = await pathOf(
         pkg,
         "run: orders -> { group_by: region; aggregate: c is count() }",
      );
      expect(report.servedBy).toEqual({ kind: "live" });
      expect(report.considered).toEqual([]);
      expect(report.notConsidered).toEqual([
         {
            tableId: entityIdOf(pkg, "by_region"),
            kind: "materialized",
            sourceName: "by_region",
            relationship: "below",
            built: true,
         },
         {
            tableId: entityIdOf(pkg, "daily"),
            kind: "materialized",
            sourceName: "daily",
            relationship: "below",
            built: false,
         },
      ]);
   });

   it("lists a sibling's table, off the queried source's chain", async () => {
      const pkg = await loadPackage();
      const report = await pathOf(
         pkg,
         "run: by_region -> { group_by: region; aggregate: n is n.sum() }",
      );
      expect(report.notConsidered).toEqual([
         {
            tableId: entityIdOf(pkg, "daily"),
            kind: "materialized",
            sourceName: "daily",
            relationship: "sibling",
            built: false,
         },
      ]);
   });
});

const ROLLUP_MODEL = `##! experimental { persistence composite_sources }
source: orders is duckdb.sql("""
  SELECT * FROM (VALUES (10, 'A'), (20, 'A'), (30, 'B')) AS t(amount, category)
""") extend {
  #@ preaggregate grain="category"
  measure: total is amount.sum()
}
`;

describe("the path report over a colocated rollup", () => {
   async function builtRollup(): Promise<{ pkg: Package; rollupId: string }> {
      const pkg = await loadPackage(ROLLUP_MODEL);
      const [rollupId] = [...pkg.getPreaggregateEntityIds()];
      const rollup = Object.values(pkg.getBuildPlan()?.sources ?? {}).find(
         (s) => s.sourceEntityId === rollupId,
      );
      if (!rollup) throw new Error("no rollup in the plan");
      const connection = (await pkg.getMalloyConnection(
         "duckdb",
      )) as DuckDBConnection;
      await connection.runSQL(
         `CREATE OR REPLACE TABLE built_rollup AS ${rollup.sql}`,
      );
      pkg.bindColocatedServeManifest({
         [rollupId]: { tableName: "built_rollup", connectionName: "duckdb" },
      } as never);
      return { pkg, rollupId };
   }

   it("names the rollup that answered", async () => {
      const { pkg, rollupId } = await builtRollup();
      const report = await pathOf(
         pkg,
         "run: orders -> { group_by: category; aggregate: total }",
      );
      expect(report.servedBy).toEqual({ kind: "rollup", tableId: rollupId });
      expect(report.considered).toEqual([
         { tableId: rollupId, kind: "rollup", outcome: "used" },
      ]);
   });

   it("skips the rollup for a count it cannot reproduce", async () => {
      const { pkg, rollupId } = await builtRollup();
      const report = await pathOf(
         pkg,
         "run: orders -> { group_by: category; aggregate: c is count() }",
      );
      expect(report.servedBy).toEqual({ kind: "live" });
      expect(report.considered).toEqual([
         {
            tableId: rollupId,
            kind: "rollup",
            outcome: "skipped",
            reason: "COUNT_SEMANTICS",
            detail: expect.stringContaining("count"),
         },
      ]);
   });

   it("skips the rollup for a group its grain does not hold", async () => {
      const { pkg, rollupId } = await builtRollup();
      const report = await pathOf(
         pkg,
         "run: orders -> { group_by: amount; aggregate: total }",
      );
      expect(report.servedBy).toEqual({ kind: "live" });
      // The companion compiles, and Malloy's composite answers from the base.
      expect(report.considered).toEqual([
         {
            tableId: rollupId,
            kind: "rollup",
            outcome: "skipped",
            reason: "GRAIN_TOO_COARSE",
         },
      ]);
   });
});
