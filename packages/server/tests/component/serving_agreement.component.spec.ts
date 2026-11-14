// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// End-to-end serving behaviour the per-surface tests missed, each found by
// walking a real package through publish, build, bind and query: a rollup on a
// source an `index.malloy` re-exports, an inherited `#@ preaggregate`, serving
// after a connection edit, and the verdicts, path report, preview and package
// warnings agreeing about one package.
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { FixedConnectionMap, MalloyConfig } from "@malloydata/malloy";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs/promises";
import * as os from "os";
import * as path from "path";
import { unrecordedAudit } from "../../src/service/access_record";
import { Environment } from "../../src/service/environment";
import { parsePackageName } from "../../src/service/identity";
import { QueryPrivileges } from "../../src/service/model_query_request";
import type { Package } from "../../src/service/package";
import { installSettings } from "../../src/test_helpers/settings_env";
import { writePackageFiles } from "../../src/test_helpers/temp_package";

let rootDir: string;
let envPath: string;
let restoreSettings: () => void;

beforeEach(async () => {
   restoreSettings = installSettings({
      env: {
         PERSIST_STORAGE_MODE: "on",
         PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true",
      },
   }).restore;
   rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "publisher-agreement-"));
   envPath = path.join(rootDir, "env");
   await fs.mkdir(envPath, { recursive: true });
});

afterEach(async () => {
   restoreSettings();
   await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
});

async function loadPackage(
   files: Record<string, string>,
   connections: unknown[] = [],
): Promise<{ env: Environment; pkg: Package }> {
   writePackageFiles(path.join(envPath, "pkg"), files, {
      manifest: { name: "pkg", description: "fixture" },
   });
   const env = await Environment.create(
      "testEnv",
      envPath,
      connections as never,
   );
   await env.addPackage("pkg");
   return { env, pkg: await env.getPackage(parsePackageName("pkg"), false) };
}

function planEntry(pkg: Package, name: string, modelPath?: string) {
   const found = Object.values(pkg.getBuildPlan()?.sources ?? {}).find(
      (s) =>
         s.name === name &&
         (modelPath === undefined || s.modelPath === modelPath),
   );
   if (!found) throw new Error(`${name} is not in the build plan`);
   return found;
}

async function run(
   pkg: Package,
   modelPath: string,
   query: string,
   options: { includeHidden?: boolean; givens?: Record<string, unknown> } = {},
) {
   const model = pkg.getModel(modelPath);
   if (!model) throw new Error(`${modelPath} did not load`);
   return model.runQuery(
      unrecordedAudit(),
      { query, ...(options.givens ? { givens: options.givens } : {}) } as never,
      QueryPrivileges.of({
         includeHiddenFilesAndSources: options.includeHidden === true,
      }),
   );
}

const ROLLUP_MODEL = `##! experimental { persistence composite_sources }
source: orders is duckdb.sql("""
  SELECT * FROM (VALUES (10, 'A'), (20, 'A'), (30, 'B')) AS t(amount, category)
""") extend {
  #@ preaggregate grain="category"
  measure: total is amount.sum()
}
`;

/** Build every planned rollup (one table per address) and bind it colocated. */
async function buildRollups(pkg: Package): Promise<string> {
   const ids = [...pkg.getPreaggregateEntityIds()];
   expect(ids).toHaveLength(1);
   const rollup = Object.values(pkg.getBuildPlan()?.sources ?? {}).find(
      (s) => s.sourceEntityId === ids[0],
   );
   if (!rollup) throw new Error("no rollup in the plan");
   const connection = (await pkg.getMalloyConnection(
      "duckdb",
   )) as DuckDBConnection;
   await connection.runSQL(
      `CREATE OR REPLACE TABLE built_rollup AS ${rollup.sql}`,
   );
   pkg.bindColocatedServeManifest({
      [ids[0]]: { tableName: "built_rollup", connectionName: "duckdb" },
   } as never);
   return ids[0];
}

describe("a rollup on a source an index.malloy re-exports", () => {
   it("is reported as the table that answered, through the published file and the declaring one", async () => {
      const { pkg } = await loadPackage({
         "model.malloy": ROLLUP_MODEL,
         "index.malloy": 'import "model.malloy"\nexport { orders }\n',
      });
      const rollupId = await buildRollups(pkg);
      for (const [modelPath, includeHidden] of [
         ["index.malloy", false],
         ["model.malloy", true],
      ] as const) {
         const result = await run(
            pkg,
            modelPath,
            "run: orders -> { group_by: category; aggregate: total }",
            { includeHidden },
         );
         // The rollup answered, and the report says so.
         expect(JSON.stringify(result)).toContain("built_rollup");
         expect(result.path?.servedBy).toEqual({
            kind: "rollup",
            tableId: rollupId,
         });
         expect(result.path?.considered).toEqual([
            { tableId: rollupId, kind: "rollup", outcome: "used" },
         ]);
      }
      // The verdict agrees with the path report.
      const verdicts = pkg
         .getServeVerdicts()
         .filter((v) => v.origin === "preaggregate");
      expect(verdicts.length).toBeGreaterThan(0);
      expect(verdicts.every((v) => v.verdict === "serves")).toBe(true);
   });
});

const INHERITED_MODEL = `##! experimental { persistence composite_sources }
source: orders is duckdb.sql("""
  SELECT * FROM (VALUES (10, 'A'), (20, 'A'), (30, 'B')) AS t(amount, category)
""") extend {
  #@ preaggregate grain="category"
  measure: total is amount.sum()
}

#@ persist name="daily"
source: daily is orders -> { group_by: category; aggregate: total }
`;

describe("a #@ preaggregate measure reused in a persisted query", () => {
   it("loads: the query's output column inherits the annotation and is not validated as misplaced", async () => {
      const { pkg } = await loadPackage({
         "model.malloy": INHERITED_MODEL,
         "index.malloy": 'import "model.malloy"\nexport { orders, daily }\n',
      });
      expect(pkg.getModel("model.malloy")?.preaggregateViolations()).toEqual(
         [],
      );
      expect(pkg.getModel("index.malloy")?.preaggregateViolations()).toEqual(
         [],
      );
      expect(pkg.formatInvalidPreaggregatePolicy()).toBe("");
      expect(planEntry(pkg, "daily").origin).not.toBe("preaggregate");
   });

   it("still refuses an annotation written on a dimension, named once, by the file that declares it", async () => {
      const refusal = await loadPackage({
         "model.malloy": `${INHERITED_MODEL}
source: misplaced is orders extend {
  #@ preaggregate grain="category"
  dimension: big is amount > 15
}
`,
         "index.malloy": 'import "model.malloy"\nexport { orders }\n',
      }).then(
         () => undefined,
         (error: unknown) =>
            error instanceof Error ? error.message : String(error),
      );
      expect(refusal).toContain("model.malloy: `big` on `misplaced`");
      expect(refusal).not.toContain("index.malloy:");
      expect(refusal?.match(/carries `#@ preaggregate`/g)).toHaveLength(1);
   });
});

/** An environment DuckDB connection; its setupSQL is part of its digest. */
function duck(setupSQL: string) {
   return { name: "duck", type: "duckdb", duckdbConnection: { setupSQL } };
}

const DUCK_MODEL = `##! experimental.persistence
#@ persist name="daily"
source: daily is duck.sql("SELECT 1 AS id, 2 AS amount") -> { select: id, amount }
`;

/** Build `daily`'s table under the plan's current address and bind it. */
async function buildDaily(pkg: Package, table: string): Promise<string> {
   const entry = planEntry(pkg, "daily");
   const connection = (await pkg.getMalloyConnection(
      "duck",
   )) as DuckDBConnection;
   await connection.runSQL(`CREATE OR REPLACE TABLE ${table} AS ${entry.sql}`);
   pkg.bindColocatedServeManifest({
      [entry.sourceEntityId]: { tableName: table, connectionName: "duck" },
   } as never);
   return entry.sourceEntityId;
}

describe("serving after a connection edit and a rebuild", () => {
   it("serves from the table built under the new address", async () => {
      const { env, pkg } = await loadPackage({ "model.malloy": DUCK_MODEL }, [
         duck("SELECT 1"),
      ]);
      const before = await buildDaily(pkg, "built_before");
      const first = await run(
         pkg,
         "model.malloy",
         "run: daily -> { select: * }",
         {
            includeHidden: true,
         },
      );
      expect(JSON.stringify(first)).toContain("built_before");

      // The edit moves the address: setupSQL is part of the connection digest.
      await env.update({
         name: "testEnv",
         connections: [duck("SELECT 2")],
      } as never);
      await pkg.whenBuildPlanCurrent();
      const after = await buildDaily(pkg, "built_after");
      expect(after).not.toBe(before);

      const second = await run(
         pkg,
         "model.malloy",
         "run: daily -> { select: * }",
         { includeHidden: true },
      );
      expect(JSON.stringify(second)).toContain("built_after");
      expect(second.path?.servedBy).toEqual({
         kind: "materialized",
         tableId: after,
         via: "served_copy",
      });
      expect(
         pkg.getServeVerdicts().find((v) => v.sourceName === "daily"),
      ).toMatchObject({ verdict: "serves" });
   });
});

const AGREEMENT_MODEL = `##! experimental { persistence composite_sources sql_functions givens }
given:
  ROLES :: string[]

source: orders is duckdb.sql("""
  SELECT * FROM (VALUES
    (1, 'east', 10, '10'), (2, 'west', 20, '20'), (3, 'east', 5, '5')
  ) AS t(id, region, amount, amount_txt)
""") extend {
  primary_key: id
  #@ preaggregate grain="region"
  measure: total is amount.sum()
}

#@ persist name="daily_c"
source: daily_c is orders -> { group_by: region; aggregate: t is amount.sum() }

#@ persist name="daily_s" storage="lake"
source: daily_s is orders -> {
  group_by: region
  aggregate: t is amount.sum(), n is count()
} extend {
  view: by_region is { group_by: region; aggregate: tt is t.sum() }
}

#(authorize) 'admin' in $ROLES
source: daily_s_secure is daily_s extend { dimension: r2 is region }

#@ persist name="txt_s" storage="lake"
source: txt_s is orders -> {
  select: id, region, amount_num is sql_number("\${amount_txt}")
}
`;

const AGREEMENT_INDEX = `import "model.malloy"
export { orders, daily_c, daily_s, daily_s_secure, txt_s }
`;

/**
 * A destination DuckDB whose catalog is named `lake`, as a storage binding's
 * table path (`lake.main.<table>`) addresses it.
 */
function lakeDestination(dir: string): {
   lake: DuckDBConnection;
   config: MalloyConfig;
} {
   const lake = new DuckDBConnection("lake", path.join(dir, "lake.duckdb"));
   const config = new MalloyConfig({ connections: {} });
   config.wrapConnections(
      () => new FixedConnectionMap(new Map([["lake", lake]]), "lake"),
   );
   return { lake, config };
}

/** Build every planned table: colocated in `duckdb`, `storage=` in `lake`. */
async function buildAll(pkg: Package, lake: DuckDBConnection): Promise<void> {
   const warehouse = (await pkg.getMalloyConnection(
      "duckdb",
   )) as DuckDBConnection;
   const colocated: Record<string, Record<string, unknown>> = {};
   const stored: Record<string, Record<string, unknown>> = {};
   const seen = new Set<string>();
   // One table per address, recorded under the source that declares it (an
   // extension inherits `#@ persist` and shares its base's table), as a build
   // records it.
   const entries = Object.values(pkg.getBuildPlan()?.sources ?? {}).sort(
      (a, b) =>
         Number(b.annotationFields?.name === b.name) -
         Number(a.annotationFields?.name === a.name),
   );
   for (const entry of entries) {
      if (seen.has(entry.sourceEntityId)) continue;
      seen.add(entry.sourceEntityId);
      const table = `t_${entry.sourceEntityId.slice(0, 12)}`;
      const storage = entry.annotationFields?.storage;
      if (storage) {
         await lake.runSQL(`CREATE OR REPLACE TABLE ${table} AS ${entry.sql}`);
         const described = await lake.runSQL(`DESCRIBE ${table}`);
         stored[entry.sourceEntityId] = {
            sourceEntityId: entry.sourceEntityId,
            sourceName: entry.name,
            physicalTableName: table,
            storageDestinationName: storage,
            ...(entry.origin === "preaggregate"
               ? { origin: "preaggregate" }
               : {}),
            schema: (
               described.rows as { column_name: string; column_type: string }[]
            ).map((r) => ({ name: r.column_name, type: r.column_type })),
         };
      } else {
         await warehouse.runSQL(
            `CREATE OR REPLACE TABLE ${table} AS ${entry.sql}`,
         );
         colocated[entry.sourceEntityId] = {
            tableName: table,
            connectionName: "duckdb",
         };
      }
   }
   pkg.bindColocatedServeManifest(colocated as never);
   pkg.bindStorageServeBindings(stored as never);
}

describe("the verdicts, path report, preview and warnings agree on one package", () => {
   it("says the same thing about every materialized source on every surface", async () => {
      const { pkg } = await loadPackage({
         "model.malloy": AGREEMENT_MODEL,
         "index.malloy": AGREEMENT_INDEX,
      });
      const { lake, config } = lakeDestination(rootDir);
      pkg.setServeDestinationConfig(() => config);
      const preview = await pkg.previewServeVerdicts();
      await buildAll(pkg, lake);

      const verdicts = pkg.getServeVerdicts();
      const verdictOf = (name: string) => {
         const found = verdicts.filter(
            (v) => v.sourceName === name && v.origin === "persist",
         );
         expect(found).toHaveLength(1);
         return found[0];
      };
      const query = (text: string, givens?: Record<string, unknown>) =>
         run(pkg, "index.malloy", text, { givens });

      // Colocated: serves, and the path report names its table.
      expect(verdictOf("daily_c").verdict).toBe("serves");
      const c = await query(
         "run: daily_c -> { group_by: region; aggregate: x is t.sum() }",
      );
      expect(c.path?.servedBy.kind).toBe("materialized");

      // storage=: serves, from storage, with its view too; the dry-run
      // preview said so before anything was built.
      expect(verdictOf("daily_s")).toMatchObject({
         verdict: "serves",
         tier: "storage",
      });
      const s = await query("run: daily_s -> by_region");
      expect(s.servedFrom).toBe("storage");
      expect(s.path?.servedBy.kind).toBe("materialized");
      expect(preview.find((v) => v.sourceName === "daily_s")?.views).toEqual([
         { view: "by_region", verdict: "serves" },
      ]);

      // The rollup on the re-exported base: serves, and the path report
      // names the rollup.
      const rollups = verdicts.filter((v) => v.origin === "preaggregate");
      expect(rollups.length).toBeGreaterThan(0);
      expect(rollups.every((v) => v.verdict === "serves")).toBe(true);
      const r = await query(
         "run: orders -> { group_by: region; aggregate: total }",
      );
      expect(r.path?.servedBy.kind).toBe("rollup");

      // The gated extension shares its base's table. Its verdict says live
      // because the gate refused it, and the path report says why the query
      // didn't use the table: WARNED with the verdict's code, `refused`
      // (decided 2026-11-11), so both surfaces name the same cause.
      const secure = verdictOf("daily_s_secure");
      expect(secure).toMatchObject({ verdict: "live", reason: "refused" });
      expect(secure.message).toContain("protected by a gate");
      const g = await query(
         "run: daily_s_secure -> { group_by: r2; aggregate: x is t.sum() }",
         { ROLES: ["admin"] },
      );
      expect(g.path?.servedBy.kind).not.toBe("materialized");
      const skipped = g.path?.considered.find(
         (entry) =>
            entry.outcome === "skipped" && entry.kind === "materialized",
      );
      expect(skipped).toMatchObject({ reason: "WARNED", code: "refused" });

      // A mistyped column: the package warns about it, the source still
      // serves, and a query reading that column is answered live.
      const warnings = pkg.getPackageMetadata().warnings ?? [];
      expect(
         warnings.filter((w) => w.code === "storage-column-type-mismatch"),
      ).toMatchObject([{ subject: "txt_s" }]);
      expect(verdictOf("txt_s").verdict).toBe("serves");
      const tOk = await query("run: txt_s -> { group_by: region }");
      expect(tOk.servedFrom).toBe("storage");
      const tLive = await query("run: txt_s -> { group_by: amount_num }");
      expect(tLive.servedFrom).not.toBe("storage");
   });
});
