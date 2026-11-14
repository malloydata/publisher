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
import { QueryPrivileges } from "../../src/service/model_query_request";
import type { Package } from "../../src/service/package";
import { __setPackageLoadPoolForTests } from "../../src/package_load/package_load_pool";
import { installSettings } from "../../src/test_helpers/settings_env";
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

async function loadPackage(
   model = MODEL,
   extraFiles: Record<string, string> = {},
): Promise<Package> {
   writePackageFiles(
      path.join(envPath, "pkg"),
      { "model.malloy": model, ...extraFiles },
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

async function pathOf(
   pkg: Package,
   query: string,
   options: { modelPath?: string; includeHidden?: boolean } = {},
) {
   const modelPath = options.modelPath ?? "model.malloy";
   const model = pkg.getModel(modelPath);
   if (!model) throw new Error(`${modelPath} did not load`);
   const { path } = await model.runQuery(
      unrecordedAudit(),
      { query },
      QueryPrivileges.of({
         includeHiddenFilesAndSources: options.includeHidden === true,
      }),
   );
   if (!path) throw new Error("no path report");
   return path;
}

describe("the path report for a model with nothing materialized", () => {
   it("answers live with the query's shape, and reads no catalog", async () => {
      const pkg = await loadPackage(`
source: orders is duckdb.sql("SELECT 1 AS id, 'east' AS region") extend {
  measure: n is count()
}
`);
      const report = await pathOf(
         pkg,
         "run: orders -> { group_by: region; aggregate: n }",
      );
      expect(report).toEqual({
         query: {
            sourceName: "orders",
            groupedBy: ["region"],
            filteredBy: [],
            measures: ["n"],
         },
         servedBy: { kind: "live" },
         considered: [],
         notConsidered: [],
         warnings: [],
      });
   });
});

describe("the access record's copy of the path report", () => {
   async function auditOf(pkg: Package, query: string) {
      const model = pkg.getModel("model.malloy");
      if (!model) throw new Error("model.malloy did not load");
      const audit = unrecordedAudit();
      await model.runQuery(audit, { query }, QueryPrivileges.of({}));
      return audit;
   }

   it("records which table answered and what was considered", async () => {
      const pkg = await loadPackage();
      await buildAndBind(pkg, { daily: {} });
      const audit = await auditOf(
         pkg,
         "run: daily -> { group_by: region; aggregate: t is total.sum() }",
      );
      // The served copy answers a v1 colocated table (the v1 cutover).
      expect(audit.path).toEqual({
         servedBy: {
            kind: "materialized",
            tableId: entityIdOf(pkg, "daily"),
            via: "served_copy",
         },
         considered: [
            {
               tableId: entityIdOf(pkg, "daily"),
               kind: "materialized",
               outcome: "used",
            },
         ],
      });
   });

   it("records nothing for a query that answered live with nothing considered", async () => {
      const pkg = await loadPackage(`
source: orders is duckdb.sql("SELECT 1 AS id, 'east' AS region") extend {
  measure: n is count()
}
`);
      const audit = await auditOf(
         pkg,
         "run: orders -> { group_by: region; aggregate: n }",
      );
      expect(audit.path).toBeUndefined();
   });
});

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
         via: "served_copy",
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

   it("never names a sibling the package's surface hides", async () => {
      // index.malloy publishes by_region only: daily is a building block.
      const pkg = await loadPackage(MODEL, {
         "index.malloy": 'import "model.malloy"\nexport { by_region }\n',
      });
      const query =
         "run: by_region -> { group_by: region; aggregate: n is n.sum() }";
      const report = await pathOf(pkg, query, { modelPath: "index.malloy" });
      expect(report.notConsidered).toEqual([]);
      expect(JSON.stringify(report)).not.toContain("daily");
      // A caller cleared to see hidden sources is told about it.
      const cleared = await pathOf(pkg, query, {
         modelPath: "index.malloy",
         includeHidden: true,
      });
      expect(cleared.notConsidered.map((n) => n.sourceName)).toEqual(["daily"]);
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

describe("the path report reading the serve verdicts", () => {
   it("says a table no published source reaches is WARNED unreachable, and names it only to a caller who may see it", async () => {
      // index.malloy publishes daily only: by_region is materialized but
      // nothing on the surface reads it, so its verdict is `unreachable`.
      const pkg = await loadPackage(MODEL, {
         "index.malloy": 'import "model.malloy"\nexport { daily }\n',
      });
      const verdict = pkg
         .getServeVerdicts()
         .find((v) => v.sourceName === "by_region");
      expect(verdict).toMatchObject({ verdict: "live", reason: "unreachable" });

      const cleared = await pathOf(
         pkg,
         "run: by_region -> { group_by: region; aggregate: n is n.sum() }",
         { includeHidden: true },
      );
      expect(cleared.considered).toEqual([
         {
            tableId: entityIdOf(pkg, "by_region"),
            kind: "materialized",
            outcome: "skipped",
            reason: "WARNED",
            code: "unreachable",
         },
      ]);
      expect(cleared.warnings).toEqual([
         {
            code: "unreachable",
            tableId: entityIdOf(pkg, "by_region"),
            sourceName: "by_region",
            message: expect.stringContaining("No source the package publishes"),
         },
      ]);

      // A published query never hears of it.
      const published = await pathOf(
         pkg,
         "run: daily -> { group_by: region; aggregate: t is total.sum() }",
         { modelPath: "index.malloy" },
      );
      expect(published.considered.map((c) => c.reason)).toEqual(["NOT_BUILT"]);
      expect(published.warnings).toEqual([]);
      expect(JSON.stringify(published)).not.toContain("by_region");
   });
});

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
            // The rollup leg's reason: the count reads the rollup's rows.
            detail: expect.stringContaining("aggregates the rollup's rows"),
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
      // The rollup leg names the field its grain does not keep.
      expect(report.considered).toEqual([
         {
            tableId: rollupId,
            kind: "rollup",
            outcome: "skipped",
            reason: "GRAIN_TOO_COARSE",
            detail: expect.stringContaining("`amount`"),
         },
      ]);
   });
});

// A host's manifest bind (Environment.bindManifest) goes through
// `reloadAllModels`, which leaves the whole unfiltered manifest on each
// model's Runtime. Malloy reads a run handed no `buildManifest` as "use the
// Runtime's", so a run that passes none substitutes a table its freshness
// window should have dropped. `bindColocatedServeManifest` stores nothing on
// the Runtime, which is why the STALE test above cannot see this.
describe("a table past its freshness window, bound by a host's reload", () => {
   let restore: () => void = () => {};

   afterEach(async () => {
      restore();
      restore = () => {};
      await __setPackageLoadPoolForTests(null);
   });

   // One mode since the v1 cutover's switch retired (6g); the name stays in
   // the test titles.
   type Mode = "on";
   async function underMode(_mode: Mode): Promise<void> {
      restore = installSettings({
         env: {
            PERSIST_STORAGE_MODE: "on",
            PUBLISHER_MATERIALIZE: "true",
         },
      }).restore;
      // A worker reads its settings at spawn, and the plan is made there.
      await __setPackageLoadPoolForTests(null);
   }

   /**
    * Create each table from `sql` (a sentinel unless it is the plan's own)
    * and bind them all as a host does, through `reloadAllModels`.
    */
   async function bindThroughReload(
      pkg: Package,
      tables: {
         entityId: string;
         table: string;
         sql: string;
         extra?: Record<string, unknown>;
      }[],
   ): Promise<void> {
      const connection = (await pkg.getMalloyConnection(
         "duckdb",
      )) as DuckDBConnection;
      const entries: Record<string, Record<string, unknown>> = {};
      for (const t of tables) {
         await connection.runSQL(
            `CREATE OR REPLACE TABLE ${t.table} AS ${t.sql}`,
         );
         entries[t.entityId] = {
            tableName: t.table,
            connectionName: "duckdb",
            ...t.extra,
         };
      }
      await pkg.reloadAllModels(entries as never);
   }

   const STALE = (fallback: "live" | "fail") => ({
      dataAsOf: "2020-01-01T00:00:00.000Z",
      freshnessWindowSeconds: 60,
      freshnessFallback: fallback,
   });

   async function run(pkg: Package, query: string) {
      const model = pkg.getModel("model.malloy");
      if (!model) throw new Error("model.malloy did not load");
      return model.runQuery(
         unrecordedAudit(),
         { query },
         QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
      );
   }

   const DAILY_QUERY =
      "run: daily -> { group_by: region; aggregate: t is total.sum(); order_by: region }";
   const DAILY_LIVE = [
      { region: "east", t: 107.5 },
      { region: "west", t: 3.5 },
   ];
   // Same columns as daily's table, values no live answer can produce.
   const DAILY_SENTINEL = `SELECT * FROM (VALUES
      (DATE '2026-01-01', 'east', 999.0, 9),
      (DATE '2026-01-02', 'west', 1.0, 1)
   ) AS t(order_date, region, total, order_count)`;

   /** Every assertion a stale table's query owes, in every mode. */
   async function expectAnsweredLive(
      pkg: Package,
      _mode: Mode,
      query: string,
      live: unknown[],
      staleId: string,
      staleTable: string,
   ): Promise<void> {
      const answers = [await run(pkg, query)];
      for (const res of answers) {
         expect(res.compactResult).toEqual(live as never);
         expect(res.result.sql).not.toContain(staleTable);
         expect(res.path?.servedBy).toEqual({ kind: "live" });
         expect(res.path?.considered).toContainEqual({
            tableId: staleId,
            kind: "materialized",
            outcome: "skipped",
            reason: "STALE",
         });
      }
   }

   for (const mode of ["on"] as const) {
      for (const fallback of ["live", "fail"] as const) {
         it(`answers live, not from the table [${mode}, fallback ${fallback}]`, async () => {
            await underMode(mode);
            const pkg = await loadPackage();
            // Live first, before any table exists.
            expect((await run(pkg, DAILY_QUERY)).compactResult).toEqual(
               DAILY_LIVE as never,
            );
            const dailyId = entityIdOf(pkg, "daily");
            await bindThroughReload(pkg, [
               {
                  entityId: dailyId,
                  table: "built_daily_sentinel",
                  sql: DAILY_SENTINEL,
                  extra: STALE(fallback),
               },
            ]);
            // `fail` degrades to live today (freshness.ts), with the switch
            // off as on: the same answer either way.
            await expectAnsweredLive(
               pkg,
               mode,
               DAILY_QUERY,
               DAILY_LIVE,
               dailyId,
               "built_daily_sentinel",
            );
         }, 60_000);
      }

      it(`still answers from a table within its window [${mode}]`, async () => {
         await underMode(mode);
         const pkg = await loadPackage();
         const dailyId = entityIdOf(pkg, "daily");
         await bindThroughReload(pkg, [
            {
               entityId: dailyId,
               table: "built_daily_sentinel",
               sql: DAILY_SENTINEL,
               extra: {
                  dataAsOf: new Date().toISOString(),
                  freshnessWindowSeconds: 3600,
                  freshnessFallback: "live",
               },
            },
         ]);
         const res = await run(pkg, DAILY_QUERY);
         expect(res.compactResult).toEqual([
            { region: "east", t: 999 },
            { region: "west", t: 1 },
         ] as never);
         expect(res.path?.servedBy).toMatchObject({
            kind: "materialized",
            tableId: dailyId,
         });
      }, 60_000);

      // A strip that leaves nothing must still hand Malloy a manifest: the
      // rollup's entry is the only fresh one, and stripping it for the
      // author's model left none, which read as "use the Runtime's".
      it(`answers live when the only fresh table is a rollup [${mode}]`, async () => {
         await underMode(mode);
         const pkg = await loadPackage(STRIP_ROLLUP_MODEL);
         const [rollupId] = [...pkg.getPreaggregateEntityIds()];
         const rollup = Object.values(pkg.getBuildPlan()?.sources ?? {}).find(
            (s) => s.sourceEntityId === rollupId,
         );
         if (!rollup) throw new Error("no rollup in the plan");
         const dailyId = entityIdOf(pkg, "daily");
         await bindThroughReload(pkg, [
            { entityId: rollupId, table: "built_rollup", sql: rollup.sql },
            {
               entityId: dailyId,
               table: "built_daily_sentinel",
               sql: DAILY_SENTINEL,
               extra: STALE("live"),
            },
         ]);
         await expectAnsweredLive(
            pkg,
            mode,
            DAILY_QUERY,
            DAILY_LIVE,
            dailyId,
            "built_daily_sentinel",
         );
      }, 60_000);

      it(`answers live when the only fresh table is a persisted view's [${mode}]`, async () => {
         await underMode(mode);
         const pkg = await loadPackage(STRIP_VIEW_MODEL);
         // A persisted view is planned as its synthesized build source.
         const viewId = entityIdOf(pkg, "orders__by_region__view");
         const view = Object.values(pkg.getBuildPlan()?.sources ?? {}).find(
            (s) => s.sourceEntityId === viewId,
         )!;
         const dailyId = entityIdOf(pkg, "daily");
         await bindThroughReload(pkg, [
            { entityId: viewId, table: "built_view", sql: view.sql },
            {
               entityId: dailyId,
               table: "built_daily_sentinel",
               sql: DAILY_SENTINEL,
               extra: STALE("live"),
            },
         ]);
         await expectAnsweredLive(
            pkg,
            mode,
            DAILY_QUERY,
            DAILY_LIVE,
            dailyId,
            "built_daily_sentinel",
         );
      }, 60_000);
   }

   // A notebook runs each cell against the cell's own model, which carries
   // `##! experimental.persistence` only through what it imports. Malloy
   // refuses a non-empty manifest on a model without the flag, so a table
   // bound for another model must not reach such a cell (#1731, ported onto
   // the v1 cutover, where the query path takes no manifest at all).
   it("runs a notebook cell whose model has no persistence flag, beside a bound table", async () => {
      await underMode("on");
      const pkg = await loadPackage(MODEL, {
         "flagless.malloy": `source: plain is duckdb.sql("SELECT 1 AS x")`,
         "persisting.malloynb": `>>>malloy\nimport "model.malloy"\n${DAILY_QUERY}\n`,
         "flagless.malloynb": `>>>malloy\nimport "flagless.malloy"\nrun: plain -> { aggregate: c is count() }\n`,
      });
      const dailyId = entityIdOf(pkg, "daily");
      await bindThroughReload(pkg, [
         {
            entityId: dailyId,
            table: "built_daily_sentinel",
            sql: DAILY_SENTINEL,
            extra: {
               dataAsOf: new Date().toISOString(),
               freshnessWindowSeconds: 3600,
               freshnessFallback: "live",
            },
         },
      ]);
      const flagless = pkg.getModel("flagless.malloynb");
      const persisting = pkg.getModel("persisting.malloynb");
      if (!flagless || !persisting) throw new Error("notebooks did not load");
      // Without the check: "A non-empty `buildManifest` was supplied, but the
      // model is missing `##! experimental.persistence`".
      const plain = await flagless.executeNotebookCell(unrecordedAudit(), 0);
      expect(plain.result).toContain('"number_value":1');
      // A cell that imports the persisting model still reads its table.
      const read = await persisting.executeNotebookCell(unrecordedAudit(), 0);
      expect(read.result).toContain("built_daily_sentinel");
   }, 60_000);
});

const ORDERS_SQL = `duckdb.sql("""
  SELECT * FROM (VALUES
    (1, DATE '2026-01-01', 'east', 100.5),
    (2, DATE '2026-01-02', 'west', 3.5),
    (3, DATE '2026-01-02', 'east', 7.0)
  ) AS t(id, order_date, region, amount)
""")`;

const DAILY_SOURCE = `#@ persist name="daily"
source: daily is orders -> {
  group_by: order_date, region
  aggregate: total is amount.sum(), order_count is count()
}
`;

const STRIP_ROLLUP_MODEL = `##! experimental { persistence composite_sources }
source: orders is ${ORDERS_SQL} extend {
  primary_key: id
  #@ preaggregate grain="region"
  measure: amount_total is amount.sum()
}

${DAILY_SOURCE}`;

const STRIP_VIEW_MODEL = `##! experimental.persistence
source: orders is ${ORDERS_SQL} extend {
  primary_key: id
  #@ persist
  view: by_region is { group_by: region; aggregate: n is count() }
}

${DAILY_SOURCE}`;
