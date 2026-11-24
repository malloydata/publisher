// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Builds in a `storage=` destination that read a stored table there rather
 * than the warehouse:
 *
 *  - a `storage=` rollup over a base stored in the same destination groups
 *    the base's table in the destination, storing the rows its planned
 *    (warehouse) build stores, and answers as live; a run of the rollup
 *    alone builds its base first; a base table it cannot read refuses it,
 *    never builds it from the warehouse;
 *  - a `#(materialize) storage=` extension whose parent is declared in
 *    another file builds over the parent's table, as a chain in one file
 *    does.
 *
 * The destination is a DuckLake catalog in a local file, with the seams
 * materialization_freshness_model.component.spec.ts fakes (the catalog
 * attach, the warehouse passthrough, the serve connection) faked the same
 * way. A statement that reads the warehouse's rows names RAW_MARK, so the
 * statements naming it count the build's warehouse reads.
 */

import { DuckDBConnection as MalloyDuckDBConnection } from "@malloydata/db-duckdb";
import { MalloyConfig } from "@malloydata/malloy";
import {
   afterEach,
   beforeEach,
   describe,
   expect,
   it,
   setDefaultTimeout,
   spyOn,
} from "bun:test";
import * as fs from "fs";
import * as path from "path";
import { unrecordedAudit } from "../../src/service/access_record";
import * as connection from "../../src/service/connection";
import * as dialects from "../../src/service/dialects";
import { recordConnectionsAsForTests } from "../../src/service/connection_type";
import { Environment } from "../../src/service/environment";
import { parsePackageName } from "../../src/service/identity";
import { MaterializationController } from "../../src/controller/materialization.controller";
import { MaterializationService } from "../../src/service/materialization/run/materialization_service";
import * as numericPrecision from "../../src/service/materialization/run/passthrough/postgres";
import { StorageBuildStrategy } from "../../src/service/materialization/run/storage_build_strategy";
import { QueryPrivileges } from "../../src/service/model_query_request";
import type {
   Materialization,
   ResourceRepository,
} from "../../src/storage/DatabaseInterface";
import { DuckDBConnection } from "../../src/storage/duckdb/DuckDBConnection";
import { __setPackageLoadPoolForTests } from "../../src/package_load/package_load_pool";
import { installSettings } from "../../src/test_helpers/settings_env";
import { writePackageFiles } from "../../src/test_helpers/temp_package";
import { openTempStorage } from "../../src/test_helpers/temp_storage";
import { serviceOverEnvironment } from "../../src/test_helpers/materialization_service_harness";

const ENV = "e1";
const ENV_ID = "env-1";
const PKG = "pkg";
const LAKE = "lake";

setDefaultTimeout(120_000);

/** Names every statement that reads the warehouse's rows. */
const RAW_MARK = "raw_rows_mark";
const RAW = `warehouse.sql("""
  SELECT id, cat, sub, amt::DOUBLE AS amt, mark FROM (VALUES
    (1, 'a', 'x', 1.5, '${RAW_MARK}'), (2, 'a', 'y', 2.0, '${RAW_MARK}'),
    (3, 'b', 'x', 4.25, '${RAW_MARK}'), (4, 'b', 'x', NULL, '${RAW_MARK}'),
    (5, 'c', 'z', -3.0, '${RAW_MARK}')
  ) AS t(id, cat, sub, amt, mark)
""")`;

/** A `storage=` base and `storage=` rollups over it. */
const ROLLUP_MODEL = `##! experimental.persistence
source: raw is ${RAW}

#@ persist storage="${LAKE}"
source: daily is raw -> {
  group_by: cat, sub
  aggregate: total is amt.sum(), n is count(), lo is amt.min()
} extend {
  #@ preaggregate grain="cat" storage=${LAKE}
  measure: grand is total.sum()
  #@ preaggregate grain="cat" storage=${LAKE}
  measure: rows is n.sum()
  #@ preaggregate grain="cat" storage=${LAKE}
  measure: least is lo.min()
}
`;
const ROLLUP_QUERY =
   "run: daily -> { group_by: cat; aggregate: grand, rows, least }";

const CHAIN_FILES = {
   "a.malloy": `source: raw is ${RAW}

#(materialize) storage="${LAKE}"
source: parent is raw extend {
  dimension: big is amt > 2
  measure: total is amt.sum()
}
`,
   "b.malloy": `import "a.malloy"

#(materialize) storage="${LAKE}"
source: child is parent extend {
  where: cat != 'c'
  dimension: label is concat(cat, '-', sub)
  measure: n is count()
}
`,
};
const CHAIN_QUERY =
   "run: child -> { group_by: label, big; aggregate: total, n }";

/** A `storage=` rollup over a `#(materialize) storage=` base, at `grain`. */
const materializeRollupModel = (grain = "cat", rollupExtra = "") => {
   const rollup = `#(rollup) grain=["${grain}"] storage="${LAKE}"${rollupExtra}`;
   return `##! experimental.persistence
source: raw is ${RAW}

#(materialize) storage="${LAKE}"
source: orders is raw extend {
  dimension: upper_cat is upper(cat)
  ${rollup}
  measure: grand is amt.sum()
  ${rollup}
  measure: rows is count()
  ${rollup}
  measure: least is amt.min()
}
`;
};
const MATERIALIZE_ROLLUP_QUERY =
   "run: orders -> { group_by: cat; aggregate: grand, rows, least }";

/** A `storage=` child in b.malloy over `parent` in a.malloy, through c.malloy. */
const THROUGH_THIRD = {
   "a.malloy": CHAIN_FILES["a.malloy"],
   "c.malloy": `import "a.malloy"
source: mid is parent extend {
  dimension: sub_up is upper(sub)
}
`,
   "b.malloy": `import "c.malloy"

#(materialize) storage="${LAKE}"
source: child is mid extend {
  where: cat != 'c'
  dimension: label is concat(cat, '-', sub_up)
  measure: n is count()
}
`,
};

/**
 * A `#@ persist` query source (joining another stored source) and an `extend`
 * of it that adds only a `where:` (gtm-funnel's `deals`): the extension runs
 * the base's query, so it builds the base's rows. The base's query reads a
 * stored source (`rates`), so both are chained builds. With a `name=` of
 * each (`names`), the two are folded onto two tables and the extension is
 * built from the base's; without, they are one table.
 */
const extendOnlyModel = (names: boolean) => {
   const name = (n: string) => (names ? ` name="${n}"` : "");
   return `##! experimental.persistence
source: raw is ${RAW}
source: rates_raw is warehouse.sql("""
  SELECT * FROM (VALUES ('a', 10.0), ('b', 20.0), ('c', 30.0)) AS t(cat, rate)
""")

#@ persist storage="${LAKE}"${name("rates_t")}
source: rates is rates_raw -> { group_by: cat; aggregate: rate is rate.max() }

source: d_raw is raw extend {
  join_one: rates on rates.cat = cat
}

#@ persist storage="${LAKE}"${name("d_incl_t")}
source: d_incl is d_raw -> {
  group_by: cat, sub, is_deleted is amt < 0, cur_rate is rates.rate
  aggregate: total is amt.sum()
}

#@ persist storage="${LAKE}"${name("deals2_t")}
source: deals2 is d_incl extend { where: not is_deleted }
`;
};
const EXTEND_ONLY_QUERY =
   "run: deals2 -> { group_by: cat, cur_rate; aggregate: t is total.sum() }";

/**
 * gtm-funnel's shape: the stored parent is a table-shaped `include` plus
 * `extend` over an unpersisted query stage, joining warehouse sources (one
 * keyed through another join), and the extension adds a `where:` and a
 * dimension. Its joins read the warehouse, so it is built from there.
 */
const GTM_SHAPE_MODEL = `##! experimental { persistence access_modifiers }
source: raw is ${RAW}
source: attrs is warehouse.sql("""
  SELECT * FROM (VALUES ('a', 1), ('b', 2), ('c', 1)) AS t(cat, company_id)
""")
source: companies is warehouse.sql("""
  SELECT * FROM (VALUES (1, 'acme'), (2, 'globex')) AS t(company_id, company_name)
""")

source: progress is raw extend {
  join_one: at is attrs on at.cat = cat
} -> {
  group_by: cat, sub, is_deleted is amt < 0, company_id is at.company_id
  aggregate: total is amt.sum()
}

#@ persist storage="${LAKE}" name="gtm_deals_including_deleted"
source: deals_including_deleted is progress include {
  public: cat, sub, is_deleted, total
} extend {
  join_one: a is attrs on a.cat = cat
  join_one: co is companies on co.company_id = a.company_id
  dimension: company is co.company_name
}

#@ persist storage="${LAKE}" name="gtm_deals"
source: deals is deals_including_deleted extend {
  where: not is_deleted
  dimension: loud is upper(company)
}
`;
/**
 * The hammer's `an-intermediate-reaching-the-warehouse-…` shape: `rollup`
 * groups a column of `regions`, a warehouse table `daily_regional` joins
 * beside the stored `daily`, so no build over `daily`'s table exists.
 */
const WAREHOUSE_JOIN_MODEL = `##! experimental.persistence
source: raw is ${RAW}
source: regions is warehouse.sql("""
  SELECT * FROM (VALUES ('a', 'east'), ('b', 'west'), ('c', 'east')) AS t(cat, region)
""")

#@ persist storage="${LAKE}"
source: daily is raw -> { group_by: cat; aggregate: total is amt.sum() }

source: daily_regional is daily extend {
  join_one: r is regions on cat = r.cat
}

#@ persist storage="${LAKE}"
source: rollup is daily_regional -> {
  group_by: region is r.region
  aggregate: grand is total.sum()
}
`;
/** The same, with a `storage=` `summary` over `rollup` through a reshape. */
const WAREHOUSE_JOIN_CHAIN_MODEL = `${WAREHOUSE_JOIN_MODEL}
source: rollup_wide is rollup -> { select: * } extend {
  dimension: big is grand > 2
}

#@ persist storage="${LAKE}"
source: summary is rollup_wide -> { aggregate: total_grand is grand.sum() }
`;
const WAREHOUSE_JOIN_QUERY =
   "run: rollup -> { select: region, grand; order_by: region }";

const GTM_SHAPE_QUERY =
   "run: deals -> { group_by: cat, sub; aggregate: t is total.sum() }";
const GTM_SHAPE_JOINED_QUERY =
   "run: deals -> { group_by: loud, sub; aggregate: t is total.sum() }";

/** Rows with every value as a string, sorted, to compare across engines. */
const asStrings = (rows: Record<string, unknown>[]) =>
   rows
      .map((r) =>
         Object.fromEntries(
            Object.entries(r).map(([k, v]) => [
               k,
               v === null ? null : String(v),
            ]),
         ),
      )
      .sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));

type Entry = {
   sourceName: string;
   schema?: { name: string; type: string }[];
   sourceEntityId: string;
   physicalTableName: string;
   storageDestinationName?: string;
   upstreamReuse?: string;
};

const plain = (rows: Record<string, unknown>[]) =>
   rows
      .map((r) =>
         Object.fromEntries(
            Object.entries(r).map(([k, v]) => [
               k,
               typeof v === "bigint" ? Number(v) : v,
            ]),
         ),
      )
      .sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));

describe.skipIf(process.platform === "win32")(
   // Skipped on Windows: the catalog file is opened by several in-process
   // DuckDB instances at once, which Windows' mandatory file locks refuse.
   "storage= builds over a stored table in their destination",
   () => {
      let root: string;
      let envPath: string;
      let lakeDir: string;
      let db: DuckDBConnection;
      let repo: ResourceRepository;
      let env: Environment;
      let service: MaterializationService;
      let restore: () => void;
      let serve: MalloyDuckDBConnection | undefined;
      const retired: MalloyDuckDBConnection[] = [];
      let opened = 0;
      let statements: string[];
      const spies: { mockRestore(): void }[] = [];
      const catalogFile = () => path.join(lakeDir, "catalog.ducklake");

      async function attachFileLake(
         session: MalloyDuckDBConnection,
         dbName: string,
         readOnly: boolean,
      ): Promise<void> {
         await session.runSQL("INSTALL ducklake");
         await session.runSQL("LOAD ducklake");
         await session.runSQL(
            `ATTACH 'ducklake:${catalogFile()}' AS ${dbName} ` +
               `(DATA_PATH '${path.join(lakeDir, "data")}/'` +
               `${readOnly ? ", READ_ONLY" : ""})`,
         );
      }

      /**
       * A fresh serve connection, so the catalog is read as it now is. The
       * previous one is retired, not closed: a bind's read of the stored
       * tables, or a served copy compiling in the background, may still be
       * on it. Every one is closed after the test.
       */
      async function reopenServe(): Promise<void> {
         if (serve) retired.push(serve);
         serve = undefined;
         const config = new MalloyConfig({ connections: {} });
         config.wrapConnections(() => ({
            lookupConnection: async (name?: string) => {
               if (name !== undefined && name !== LAKE) {
                  throw new Error(`no destination ${name}`);
               }
               if (!serve) {
                  const c = new MalloyDuckDBConnection(
                     LAKE,
                     path.join(root, `serve_${++opened}.duckdb`),
                  );
                  await attachFileLake(c, LAKE, true);
                  serve = c;
               }
               return serve;
            },
         }));
         const pkg = await env.getPackage(parsePackageName(PKG), false);
         pkg.materialization().setServeDestinationConfig(() => config);
      }

      async function setUp(files: Record<string, string>): Promise<void> {
         writePackageFiles(path.join(envPath, PKG), files, {
            manifest: { name: PKG, description: "fixture" },
         });
         env = await Environment.create(
            ENV,
            envPath,
            [
               {
                  name: "warehouse",
                  type: "duckdb",
                  duckdbConnection: { setupSQL: "SELECT 1" },
               } as never,
            ],
            [
               {
                  name: LAKE,
                  type: "ducklake",
                  ducklakeConnection: {
                     catalog: {
                        postgresConnection: {
                           host: "h",
                           databaseName: "d",
                           userName: "u",
                        },
                     },
                     storage: { bucketUrl: lakeDir },
                  },
               } as never,
            ],
         );
         const apiConnection = env.getApiConnection.bind(env);
         spies.push(
            spyOn(env, "getApiConnection").mockImplementation((name) =>
               name === "warehouse"
                  ? ({
                       name: "warehouse",
                       type: "postgres",
                       postgresConnection: {
                          host: "h",
                          databaseName: "d",
                          userName: "u",
                       },
                    } as never)
                  : apiConnection(name),
            ),
         );
         await env.addPackage(PKG);
         await reopenServe();
         service = serviceOverEnvironment(
            () => repo,
            () => env,
         );
      }

      beforeEach(async () => {
         restore = installSettings({
            env: {
               PERSIST_STORAGE_MODE: "on",
               PUBLISHER_MATERIALIZE: "true",
               PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true",
            },
         }).restore;
         await __setPackageLoadPoolForTests(null);
         const storage = await openTempStorage("storage-stacking-", {
            id: ENV_ID,
            name: ENV,
         });
         ({ root, db, repo, envPath } = storage);
         lakeDir = path.join(root, "lake");
         fs.mkdirSync(path.join(lakeDir, "data"), { recursive: true });
         statements = [];
         const runSQL = MalloyDuckDBConnection.prototype.runSQL;
         spies.push(
            spyOn(
               MalloyDuckDBConnection.prototype,
               "runSQL",
            ).mockImplementation(function (
               this: MalloyDuckDBConnection,
               sql: string,
               ...rest: unknown[]
            ) {
               statements.push(sql);
               return (runSQL as (...a: unknown[]) => unknown).call(
                  this,
                  sql,
                  ...rest,
               );
            } as never),
            spyOn(connection, "attachDuckLakeReadWrite").mockImplementation(
               async (session, dbName) => {
                  await attachFileLake(session as never, dbName, false);
               },
            ),
            spyOn(connection, "attachDuckLakeReadOnly").mockImplementation(
               async (session, dbName) => {
                  await attachFileLake(session as never, dbName, true);
               },
            ),
            spyOn(
               connection,
               "attachDuckLakeReadOnlyIfPresent",
            ).mockImplementation(async (session, dbName) => {
               if (!fs.existsSync(catalogFile())) return false;
               await attachFileLake(session as never, dbName, true);
               return true;
            }),
            spyOn(dialects, "federateSourceForPassthrough").mockImplementation(
               async (session, sourceType) => {
                  await session.runSQL(
                     "CREATE OR REPLACE TEMP MACRO postgres_query(alias, q) AS TABLE SELECT * FROM query(q)",
                  );
                  return { handle: "warehouse", sourceType };
               },
            ),
            // The stand-in warehouse is DuckDB, which has no Postgres type
            // catalog to probe; it holds no numeric to read as text.
            spyOn(
               numericPrecision,
               "postgresNumericColumns",
            ).mockImplementation(async () => ({ columns: [], numeric: [] })),
            {
               mockRestore: recordConnectionsAsForTests({
                  warehouse: "postgres",
               }),
            },
         );
      });

      afterEach(async () => {
         for (const spy of spies.splice(0)) spy.mockRestore();
         await serve?.close().catch(() => undefined);
         serve = undefined;
         for (const c of retired.splice(0)) {
            await c.close().catch(() => undefined);
         }
         await env?.closeAllConnections().catch(() => undefined);
         await db?.close();
         fs.rmSync(root, { recursive: true, force: true });
         restore?.();
         await __setPackageLoadPoolForTests(null);
      });

      async function build(
         options: Record<string, unknown> = {},
      ): Promise<Materialization> {
         statements.length = 0;
         const created = await service.createMaterialization(ENV, PKG, options);
         const deadline = Date.now() + 120_000;
         for (;;) {
            const m = await repo.getMaterializationById(created.id);
            if (
               m &&
               ["MANIFEST_FILE_READY", "FAILED", "CANCELLED"].includes(m.status)
            ) {
               const running = (
                  service as unknown as {
                     runningAbortControllers: Map<string, unknown>;
                  }
               ).runningAbortControllers;
               while (running.has(created.id)) {
                  await new Promise((resolve) => setTimeout(resolve, 10));
               }
               const settled = (await repo.getMaterializationById(created.id))!;
               await reopenServe();
               return settled;
            }
            if (Date.now() > deadline) throw new Error("run never settled");
            await new Promise((resolve) => setTimeout(resolve, 20));
         }
      }

      /** A run another caller started, to its settled record. */
      async function settled(id: string): Promise<Materialization> {
         const deadline = Date.now() + 120_000;
         for (;;) {
            const m = await repo.getMaterializationById(id);
            if (
               m &&
               ["MANIFEST_FILE_READY", "FAILED", "CANCELLED"].includes(m.status)
            ) {
               const running = (
                  service as unknown as {
                     runningAbortControllers: Map<string, unknown>;
                  }
               ).runningAbortControllers;
               while (running.has(id)) {
                  await new Promise((resolve) => setTimeout(resolve, 10));
               }
               return (await repo.getMaterializationById(id))!;
            }
            if (Date.now() > deadline) throw new Error("run never settled");
            await new Promise((resolve) => setTimeout(resolve, 20));
         }
      }

      const entriesOf = (m: Materialization) =>
         Object.values(
            (m.manifest as { entries: Record<string, Entry> }).entries,
         );
      const entryOf = (m: Materialization, name: string | RegExp) => {
         const hit = entriesOf(m).find((e) =>
            typeof name === "string"
               ? e.sourceName === name
               : name.test(e.sourceName),
         );
         if (!hit) throw new Error(`no entry for ${String(name)}`);
         return hit;
      };
      /** The statements this run read the warehouse's rows with. */
      const warehouseReads = () =>
         statements.filter(
            (s) => s.includes(RAW_MARK) && /\bCREATE\b/i.test(s),
         );
      /** The statement that created `table`. */
      const createdBy = (table: string) => {
         const hit = statements.find(
            (s) => /\bCREATE\b/i.test(s) && s.includes(table),
         );
         if (!hit) throw new Error(`no CREATE of ${table}`);
         return hit;
      };

      /** Rows of a stored table, read from the catalog. */
      async function storedRows(table: string) {
         const c = new MalloyDuckDBConnection(
            "reader",
            path.join(root, `reader_${++opened}.duckdb`),
         );
         try {
            await attachFileLake(c, "rd", true);
            const result = await c.runSQL(`SELECT * FROM rd."${table}"`);
            return plain(result.rows as Record<string, unknown>[]);
         } finally {
            await c.close();
         }
      }

      /** Rows of the planned (warehouse) build of the table `entry` names. */
      async function plannedRows(entry: Entry) {
         const pkg = await env.getPackage(parsePackageName(PKG), false);
         const planned = Object.values(pkg.getBuildPlan()!.sources).find(
            (s) => s.sourceEntityId === entry.sourceEntityId,
         )!;
         expect(planned.sql).toContain(RAW_MARK);
         const c = new MalloyDuckDBConnection(
            "planned",
            path.join(root, `planned_${++opened}.duckdb`),
         );
         try {
            const result = await c.runSQL(planned.sql);
            return plain(result.rows as Record<string, unknown>[]);
         } finally {
            await c.close();
         }
      }

      async function answer(model: string, query: string) {
         const pkg = await env.getPackage(parsePackageName(PKG), false);
         const result = await pkg
            .getModel(model)!
            .runQuery(
               unrecordedAudit(),
               { query } as never,
               QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
            );
         return {
            rows: plain(result.compactResult as Record<string, unknown>[]),
            servedBy: result.path?.servedBy?.kind ?? "live",
         };
      }

      it("builds a storage= rollup over its base's stored table, reading the warehouse once for both", async () => {
         await setUp({ "m.malloy": ROLLUP_MODEL });
         const live = await answer("m.malloy", ROLLUP_QUERY);
         expect(live.servedBy).toBe("live");
         const m = await build();
         expect([m.status, m.error]).toEqual(["MANIFEST_FILE_READY", null]);
         const base = entryOf(m, "daily");
         const rollup = entryOf(m, /^daily__rollup__/);
         expect(rollup.storageDestinationName).toBe(LAKE);
         expect(rollup.upstreamReuse).toBe("reused");
         // The base's table it grouped, named as what it was built from.
         expect(
            (
               rollup as Entry & { builtFrom?: { sourceEntityId: string }[] }
            ).builtFrom?.map((b) => b.sourceEntityId),
         ).toEqual([base.sourceEntityId]);
         // Only the base's build read the warehouse.
         expect(warehouseReads()[0]).toContain(base.physicalTableName);
         expect(createdBy(rollup.physicalTableName)).not.toContain(RAW_MARK);
         // The warehouse build's rows, and live's answer.
         expect(await storedRows(rollup.physicalTableName)).toEqual(
            await plannedRows(rollup),
         );
         const served = await answer("m.malloy", ROLLUP_QUERY);
         expect(served.servedBy).toBe("rollup");
         expect(served.rows).toEqual(live.rows);
      });

      it("records each stored table's column contract, read back as a bind reads it", async () => {
         await setUp({ "m.malloy": ROLLUP_MODEL });
         const m = await build();
         expect([m.status, m.error]).toEqual(["MANIFEST_FILE_READY", null]);
         const base = entryOf(m, "daily") as Entry & { contract?: unknown };
         const rollup = entryOf(m, /^daily__rollup__/) as Entry & {
            contract?: unknown;
         };
         const column = (e: { contract?: unknown }, name: string) =>
            (
               e.contract as {
                  columns: { name: string; [k: string]: unknown }[];
               }
            ).columns.find((c) => c.name === name);
         expect(base.contract).toMatchObject({ v: 1, dialect: "duckdb" });
         // A string the warehouse compares byte-wise, stored byte-wise.
         expect(column(base, "cat")).toEqual({
            name: "cat",
            nativeType: "VARCHAR",
            collation: "binary",
            compare: "exact",
            gateExact: true,
            declaredType: "string",
         });
         // A double the build computed (a sum): it copies no warehouse
         // double, so no gate over it is exact.
         expect(column(base, "total")).toMatchObject({
            compare: "exact",
            gateExact: false,
         });
         expect(column(base, "n")).toMatchObject({ gateExact: true });
         // Stacked on the base's table: its strings compare as the base's.
         expect(rollup.upstreamReuse).toBe("reused");
         expect(column(rollup, "cat")).toMatchObject({
            collation: "binary",
            compare: "exact",
         });
      });

      it("binds every stored table a build wrote as its contract recorded it: none incompatible or missing its contract", async () => {
         await setUp({ "m.malloy": ROLLUP_MODEL });
         const m = await build();
         expect([m.status, m.error]).toEqual(["MANIFEST_FILE_READY", null]);
         const pkg = await env.getPackage(parsePackageName(PKG), false);
         const judged = pkg
            .materialization()
            .serveVerdicts()
            .filter(
               (v) =>
                  v.reason === "incompatible" ||
                  v.reason === "contract_missing",
            );
         expect(judged).toEqual([]);
         expect(
            pkg
               .materialization()
               .serveVerdicts()
               .some((v) => v.tier === "storage" && v.verdict === "serves"),
         ).toBe(true);
      });

      it("builds a storage= rollup's base first when a run names only the rollup, and groups its table", async () => {
         await setUp({ "m.malloy": ROLLUP_MODEL });
         const pkg = await env.getPackage(parsePackageName(PKG), false);
         const rollupName = Object.values(pkg.getBuildPlan()!.sources).find(
            (s) => s.origin === "rollup",
         )!.name;
         const m = await build({ sourceNames: [rollupName] });
         expect([m.status, m.error]).toEqual(["MANIFEST_FILE_READY", null]);
         const rollup = entryOf(m, /^daily__rollup__/);
         const base = entryOf(m, "daily");
         expect(rollup.upstreamReuse).toBe("reused");
         expect(createdBy(rollup.physicalTableName)).not.toContain(RAW_MARK);
         expect(createdBy(rollup.physicalTableName)).toContain(
            base.physicalTableName,
         );
         expect(await storedRows(rollup.physicalTableName)).toEqual(
            await plannedRows(rollup),
         );
      });

      it("reads the base's table carried from an earlier run, and a windowed rollup rebuilds one of unknown age and reads it", async () => {
         await setUp({ "m.malloy": ROLLUP_MODEL });
         const first = await build();
         expect(first.status).toBe("MANIFEST_FILE_READY");
         const regrain = async (line: string) => {
            fs.writeFileSync(
               path.join(envPath, PKG, "m.malloy"),
               ROLLUP_MODEL.replace(
                  /#@ preaggregate grain="cat" storage=lake/g,
                  line,
               ),
            );
            await env.getPackage(parsePackageName(PKG), true);
            await reopenServe();
         };
         // A new grain: only the rollup is rebuilt, over the carried base.
         await regrain('#@ preaggregate grain="sub" storage=lake');
         const second = await build();
         expect([second.status, second.error]).toEqual([
            "MANIFEST_FILE_READY",
            null,
         ]);
         const rollup = entryOf(second, /^daily__rollup__/);
         expect(rollup.upstreamReuse).toBe("reused");
         expect(warehouseReads()).toHaveLength(0);
         expect(await storedRows(rollup.physicalTableName)).toEqual(
            await plannedRows(rollup),
         );
         // A windowed rollup over that base, whose table is of unknown age
         // (it declares no window): the rollup's edge to its base rebuilds
         // the base (its one warehouse read), and the rollup reads it, so
         // it is never fresher than the rows it holds.
         await regrain(
            '#@ preaggregate grain="sub" storage=lake freshness.window="1h"',
         );
         const third = await build();
         expect([third.status, third.error]).toEqual([
            "MANIFEST_FILE_READY",
            null,
         ]);
         const windowed = entryOf(third, /^daily__rollup__/);
         expect(windowed.upstreamReuse).toBe("reused");
         expect(createdBy(windowed.physicalTableName)).not.toContain(RAW_MARK);
         expect(await storedRows(windowed.physicalTableName)).toEqual(
            await plannedRows(windowed),
         );
      });

      it("refuses a storage= rollup as a missing upstream when its base's build fails in the same run", async () => {
         await setUp({ "m.malloy": ROLLUP_MODEL });
         const build_ = StorageBuildStrategy.prototype.build;
         spies.push(
            spyOn(StorageBuildStrategy.prototype, "build").mockImplementation(
               async function (this: StorageBuildStrategy, params) {
                  if (params.persistSource.name === "daily") {
                     throw new Error("the base's build failed");
                  }
                  return build_.call(this, params);
               },
            ),
         );
         const m = await build();
         expect(m.status).toBe("FAILED");
         const failures = Object.values(
            (
               m.manifest as {
                  failures?: Record<
                     string,
                     { sourceName?: string; reason: string }
                  >;
               } | null
            )?.failures ?? {},
         );
         const rollup = failures.find((f) =>
            /^daily__rollup__/.test(f.sourceName ?? ""),
         );
         expect(rollup?.reason).toContain(
            "a rollup is built over its base's table or not at all",
         );
         expect(
            statements.some((q) => q.includes(RAW_MARK) && /rollup/.test(q)),
         ).toBe(false);
      });

      it("builds a storage= extension over its parent's stored table in another file", async () => {
         await setUp(CHAIN_FILES);
         const live = await answer("b.malloy", CHAIN_QUERY);
         expect(live.servedBy).toBe("live");
         const m = await build();
         expect([m.status, m.error]).toEqual(["MANIFEST_FILE_READY", null]);
         const parent = entryOf(m, "parent");
         const child = entryOf(m, "child");
         expect(child.upstreamReuse).toBe("reused");
         expect(createdBy(child.physicalTableName)).not.toContain(RAW_MARK);
         expect(createdBy(child.physicalTableName)).toContain(
            parent.physicalTableName,
         );
         expect(await storedRows(child.physicalTableName)).toEqual(
            await plannedRows(child),
         );
         const served = await answer("b.malloy", CHAIN_QUERY);
         expect(served.servedBy).toBe("materialized");
         expect(served.rows).toEqual(live.rows);
      });

      /** The child stacked on the parent's table: never the warehouse. */
      async function expectStackedChild(m: Materialization) {
         expect([m.status, m.error]).toEqual(["MANIFEST_FILE_READY", null]);
         const parent = entryOf(m, "parent");
         const child = entryOf(m, "child");
         expect(child.upstreamReuse).toBe("reused");
         expect(createdBy(child.physicalTableName)).not.toContain(RAW_MARK);
         expect(createdBy(child.physicalTableName)).toContain(
            parent.physicalTableName,
         );
         expect(await storedRows(child.physicalTableName)).toEqual(
            await plannedRows(child),
         );
      }

      /** The child built from the warehouse, with the warehouse build's rows. */
      /**
       * `child` is refused at plan as `upstream_table_unusable`, naming its
       * parent, and never built from the warehouse; the parent still builds.
       */
      async function expectRefusedChild(m: Materialization) {
         expect([m.status, m.error]).toEqual(["MANIFEST_FILE_READY", null]);
         const entries = Object.values(
            (m.manifest as { entries: Record<string, { sourceName: string }> })
               .entries,
         );
         expect(entries.map((e) => e.sourceName)).not.toContain("child");
         expect(entries.map((e) => e.sourceName)).toContain("parent");
         const pkg = await env.getPackage(parsePackageName(PKG), false);
         const refusal = Object.values(
            pkg.getBuildPlan()?.refusedSources ?? {},
         ).find((r) => r.name === "child");
         expect(refusal?.reason).toBe("upstream_table_unusable");
         expect(refusal?.message).toContain("it extends 'parent'");
         expect(refusal?.message).toContain(
            "never by recomputing its parent from the warehouse",
         );
      }

      const THIRD_QUERY =
         "run: child -> { group_by: label, big; aggregate: total, n }";

      it("builds a storage= extension over its parent's stored table through a third file", async () => {
         await setUp(THROUGH_THIRD);
         const live = await answer("b.malloy", THIRD_QUERY);
         expect(live.servedBy).toBe("live");
         await expectStackedChild(await build());
         const served = await answer("b.malloy", THIRD_QUERY);
         expect(served.rows).toEqual(live.rows);
      });

      it("builds a storage= extension over its parent's stored table through two files between", async () => {
         await setUp({
            "a.malloy": CHAIN_FILES["a.malloy"],
            "c.malloy": THROUGH_THIRD["c.malloy"],
            "d.malloy": `import "c.malloy"
source: mid2 is mid extend {
  measure: avg_amt is amt.avg()
}
`,
            "b.malloy": THROUGH_THIRD["b.malloy"]
               .replace('import "c.malloy"', 'import "d.malloy"')
               .replace("child is mid extend", "child is mid2 extend"),
         });
         const query =
            "run: child -> { group_by: label; aggregate: total, n, avg_amt }";
         const live = await answer("b.malloy", query);
         await expectStackedChild(await build());
         const served = await answer("b.malloy", query);
         expect(served.rows).toEqual(live.rows);
      });

      it("refuses a storage= extension at plan, never building it from the warehouse, when a file between reads what its parent's stored table does not hold", async () => {
         // `mid` reads a join of the parent's, which the destination does not
         // hold: the chain does not compile over the parent's stored table.
         await setUp({
            ...THROUGH_THIRD,
            "a.malloy": `source: raw is ${RAW}
source: cats is raw -> { group_by: cat } extend { primary_key: cat }

#(materialize) storage="${LAKE}"
source: parent is raw extend {
  join_one: k is cats on cat = k.cat
  dimension: big is amt > 2
  measure: total is amt.sum()
}
`,
            "c.malloy": `import "a.malloy"
source: mid is parent extend {
  dimension: sub_up is upper(k.cat)
}
`,
         });
         const live = await answer("b.malloy", THIRD_QUERY);
         await expectRefusedChild(await build());
         // Answered live, equal to live.
         const served = await answer("b.malloy", THIRD_QUERY);
         expect(served.rows).toEqual(live.rows);
      });

      it("refuses a storage= extension at plan, never building it from the warehouse, when a file on the chain imports the next one selectively", async () => {
         await setUp({
            ...THROUGH_THIRD,
            "b.malloy": THROUGH_THIRD["b.malloy"].replace(
               'import "c.malloy"',
               'import { mid } from "c.malloy"',
            ),
         });
         await expectRefusedChild(await build());
      });

      it("builds a storage= rollup over its #(materialize) base's stored table, reading the warehouse once for both", async () => {
         await setUp({ "m.malloy": materializeRollupModel() });
         const live = await answer("m.malloy", MATERIALIZE_ROLLUP_QUERY);
         expect(live.servedBy).toBe("live");
         const m = await build();
         expect([m.status, m.error]).toEqual(["MANIFEST_FILE_READY", null]);
         const base = entryOf(m, "orders");
         const rollup = entryOf(m, /^orders__rollup__/);
         expect(rollup.storageDestinationName).toBe(LAKE);
         expect(rollup.upstreamReuse).toBe("reused");
         expect(warehouseReads()[0]).toContain(base.physicalTableName);
         expect(createdBy(rollup.physicalTableName)).not.toContain(RAW_MARK);
         expect(createdBy(rollup.physicalTableName)).toContain(
            base.physicalTableName,
         );
         expect(await storedRows(rollup.physicalTableName)).toEqual(
            await plannedRows(rollup),
         );
         const served = await answer("m.malloy", MATERIALIZE_ROLLUP_QUERY);
         expect(served.servedBy).toBe("rollup");
         expect(served.rows).toEqual(live.rows);
      });

      /**
       * Build once, record the base's carried table as holding `column` as
       * `type`, and regrain the rollup, so the next run rebuilds the rollup
       * alone over that carried table.
       */
      async function carriedBaseHolding(column: string, type: string) {
         await setUp({ "m.malloy": materializeRollupModel() });
         const first = await build();
         expect(first.status).toBe("MANIFEST_FILE_READY");
         const manifest = first.manifest as {
            entries: Record<
               string,
               Entry & { schema?: { name: string; type: string }[] }
            >;
         };
         for (const e of Object.values(manifest.entries)) {
            if (e.sourceName !== "orders") continue;
            e.schema = e.schema!.map((c) =>
               c.name === column ? { ...c, type } : c,
            );
         }
         await repo.updateMaterialization(first.id, { manifest } as never);
         fs.writeFileSync(
            path.join(envPath, PKG, "m.malloy"),
            materializeRollupModel("sub"),
         );
         await env.getPackage(parsePackageName(PKG), true);
         await reopenServe();
         return build();
      }

      /**
       * The rollup refused over its base's carried table, as
       * `schema_incompatible`, and the next run rebuilding that table and
       * grouping it: never built from the warehouse.
       */
      async function expectRefusedThenRebuilt(m: Materialization) {
         expect(m.status).toBe("FAILED");
         const failures = Object.values(
            (
               m.manifest as {
                  failures?: Record<string, { code?: string }>;
               } | null
            )?.failures ?? {},
         );
         expect(failures.map((f) => f.code)).toEqual(["schema_incompatible"]);
         const next = await build();
         expect([next.status, next.error]).toEqual([
            "MANIFEST_FILE_READY",
            null,
         ]);
         const base = entryOf(next, "orders");
         const rollup = entryOf(next, /^orders__rollup__/);
         expect(rollup.upstreamReuse).toBe("reused");
         expect(createdBy(rollup.physicalTableName)).toContain(
            base.physicalTableName,
         );
         expect(await storedRows(rollup.physicalTableName)).toEqual(
            await plannedRows(rollup),
         );
      }

      it("refuses a storage= rollup over its #(materialize) base's carried table holding another type, and rebuilds that table next", async () => {
         // Text, where the planned build writes a number.
         await expectRefusedThenRebuilt(
            await carriedBaseHolding("amt", "VARCHAR"),
         );
      });

      it("refuses a storage= rollup over a #(materialize) base's table holding a 128-bit integer", async () => {
         // DuckLake holds a HUGEINT as a DOUBLE: its digits are rounded.
         // Its stored digits are not what the planned build writes, so it is
         // refused (chained_shape_not_carried), never built from the
         // warehouse in its place.
         const m = await carriedBaseHolding("id", "HUGEINT");
         expect(m.status).toBe("FAILED");
         const failures = Object.values(
            (
               m.manifest as {
                  failures?: Record<string, { reason: string }>;
               } | null
            )?.failures ?? {},
         );
         expect(failures).toHaveLength(1);
         expect(failures[0].reason).toContain("was not built over its base");
         expect(failures[0].reason).toContain(
            "a downstream is never built by recomputing its upstreams from raw",
         );
      });

      it("never stacks a storage= rollup on a gated #(materialize) base's table: it is refused at run, access_filter", async () => {
         await setUp({
            "m.malloy": materializeRollupModel()
               .replace(
                  "##! experimental.persistence",
                  "##! experimental { persistence givens }\ngiven: CAT :: string",
               )
               .replace(
                  '#(materialize) storage="lake"',
                  '#(access_filter) cat = $CAT\n#(materialize) storage="lake"',
               ),
         });
         const m = await build();
         expect([m.status, m.error]).toEqual(["MANIFEST_FILE_READY", null]);
         // Its rollups are refused (`access_filter`) and answer live: none
         // is built, from the base's table or from the warehouse.
         expect(
            entriesOf(m).filter((e) => /^orders__rollup__/.test(e.sourceName)),
         ).toEqual([]);
         const refused = Object.values(
            (
               m.metadata as {
                  refusedSources?: Record<
                     string,
                     { name: string; reason: string }
                  >;
               } | null
            )?.refusedSources ?? {},
         );
         expect(
            refused
               .filter((r) => /^orders__rollup__/.test(r.name))
               .map((r) => r.reason),
         ).toEqual(["access_filter"]);
      });

      it("refuses an extend-only #@ persist source named apart from its base whose base joins a stored table to the warehouse: never recomputed, answered live", async () => {
         await setUp({ "m.malloy": extendOnlyModel(true) });
         const live = await answer("m.malloy", EXTEND_ONLY_QUERY);
         expect(live.servedBy).toBe("live");
         // `d_incl` joins the stored `rates` to the warehouse's `raw`: no
         // engine reads both, so it has no build over its stored upstream,
         // and it is refused rather than recomputed from raw: at plan, its
         // grouped strings being read from the warehouse, which the refusal
         // names; `deals2`, over it, with it.
         const m = await build({ strictUpstreams: true });
         const pkg = await env.getPackage(parsePackageName(PKG), false);
         const refusals = Object.values(
            pkg.getBuildPlan()?.refusedSources ?? {},
         );
         const whyNot = (name: string) =>
            refusals.find((r) => r.name === name)?.message ??
            (
               entriesOf(m).find((e) => e.sourceName === name) as
                  | { error?: string }
                  | undefined
            )?.error ??
            "";
         expect(whyNot("d_incl")).toContain(
            "it reads the source warehouse (raw) through 'd_raw'",
         );
         for (const name of ["d_incl", "deals2"]) {
            expect(whyNot(name)).toContain(
               "a downstream is never built by recomputing its upstreams from raw",
            );
         }
         const served = await answer("m.malloy", EXTEND_ONLY_QUERY);
         expect(served.servedBy).toBe("live");
         expect(asStrings(served.rows)).toEqual(asStrings(live.rows));
      });

      it("reads a host's reference to a stored upstream as stored, through the controller: stacked with its schema, refused without it, never recomputed", async () => {
         await setUp({
            "m.malloy": `##! experimental.persistence
source: raw is ${RAW}

#@ persist storage="${LAKE}" name="ref_daily_t"
source: daily is raw -> { group_by: cat, sub; aggregate: total is amt.sum() }

#@ persist storage="${LAKE}" name="ref_monthly_t"
source: monthly is daily -> { group_by: cat; aggregate: grand is total.sum() }

source: monthly_live is raw -> {
  group_by: cat, sub; aggregate: total is amt.sum()
} -> { group_by: cat; aggregate: grand is total.sum() }
`,
         });
         const first = await build();
         expect([first.status, first.error]).toEqual([
            "MANIFEST_FILE_READY",
            null,
         ]);
         const daily = entryOf(first, "daily");
         const monthly = entryOf(first, "monthly");
         expect(daily.storageDestinationName).toBe(LAKE);
         // No run on record holds `daily` here, as on a host's worker that
         // never built it and binds no manifest: the reference alone says
         // where its table is.
         await repo.deleteMaterialization(first.id);
         // The host rebuilds `monthly` alone, `daily` by reference, as
         // Credible's control plane sends it: through the HTTP controller,
         // which must keep the reference's destination. Before, it dropped
         // it, so the reference read as a warehouse table and the build
         // failed as a host-supplied upstream at the wrong tier.
         const host = async (
            table: string,
            strictUpstreams: boolean,
            schema?: unknown,
            contract?: unknown,
         ) => {
            const created = await new MaterializationController(
               service,
            ).createMaterialization(ENV, PKG, {
               buildInstructions: {
                  sources: [
                     {
                        sourceEntityId: monthly.sourceEntityId,
                        materializedTableId: `host-${table}`,
                        physicalTableName: table,
                        realization: "COPY",
                        destination: LAKE,
                     },
                  ],
                  referenceManifest: [
                     {
                        sourceEntityId: daily.sourceEntityId,
                        physicalTableName: daily.physicalTableName,
                        connectionName: "warehouse",
                        storageDestinationName: LAKE,
                        ...(schema ? { schema } : {}),
                        ...(contract ? { contract } : {}),
                     },
                  ],
                  strictUpstreams,
               },
            });
            return settled(created.id);
         };
         // A reference that carries no column list, which stacking on a
         // stored table needs (a bound manifest or this worker's own record
         // fills it): refused, strict or not, and says why; never recomputed.
         for (const [table, strictUpstreams] of [
            ["host_monthly_t", false],
            ["host_monthly_strict_t", true],
         ] as const) {
            const refused = await host(table, strictUpstreams);
            expect(refused.status).toBe("FAILED");
            expect(refused.error).toContain(
               "'daily' is materialized in destination 'lake', but by a reference that carries no column list",
            );
         }
         // With the schema the host stored from daily's entry, but not its
         // contract, the strings `monthly` groups cannot be judged: refused.
         const unjudged = await host(
            "host_monthly_no_contract_t",
            true,
            daily.schema,
         );
         expect(unjudged.status).toBe("FAILED");
         expect(unjudged.error).toContain(
            "an upstream it reads has no column contract",
         );
         // With the schema and the contract the host stored, it stacks.
         const stacked = await host(
            "host_monthly_schema_t",
            true,
            daily.schema,
            (daily as { contract?: unknown }).contract,
         );
         expect([stacked.status, stacked.error]).toEqual([
            "MANIFEST_FILE_READY",
            null,
         ]);
         const rebuilt = entryOf(stacked, "monthly");
         expect(rebuilt.upstreamReuse).toBe("reused");
         const live = await answer(
            "m.malloy",
            "run: monthly_live -> { group_by: cat, grand }",
         );
         expect(asStrings(await storedRows(rebuilt.physicalTableName))).toEqual(
            asStrings(live.rows),
         );
      });

      it("refuses a host's reference to a storage destination the environment does not configure", async () => {
         await setUp({
            "m.malloy": `##! experimental.persistence
source: raw is ${RAW}

#@ persist storage="${LAKE}"
source: daily is raw -> { group_by: cat; aggregate: total is amt.sum() }

#@ persist storage="${LAKE}"
source: monthly is daily -> { group_by: cat; aggregate: grand is total.sum() }
`,
         });
         const pkg = await env.getPackage(parsePackageName(PKG), false);
         const sources = Object.values(pkg.getBuildPlan()!.sources);
         const of = (name: string) => sources.find((s) => s.name === name)!;
         await expect(
            new MaterializationController(service).createMaterialization(
               ENV,
               PKG,
               {
                  buildInstructions: {
                     sources: [
                        {
                           sourceEntityId: of("monthly").sourceEntityId,
                           materializedTableId: "host-monthly",
                           physicalTableName: "host_monthly_t",
                           destination: LAKE,
                        },
                     ],
                     referenceManifest: [
                        {
                           sourceEntityId: of("daily").sourceEntityId,
                           physicalTableName: "daily_t",
                           storageDestinationName: "nowhere",
                        },
                     ],
                  },
               },
            ),
         ).rejects.toThrow(
            "names storage destination 'nowhere', which this environment does not configure",
         );
      });

      it("refuses at plan, bound or not, a storage= source reading a warehouse join beside its stored upstream, naming the warehouse table; a host can never instruct it into a silent success", async () => {
         await setUp({ "m.malloy": WAREHOUSE_JOIN_MODEL });
         const live = await answer("m.malloy", WAREHOUSE_JOIN_QUERY);
         const refusalOf = async () => {
            const pkg = await env.getPackage(parsePackageName(PKG), false);
            const plan = pkg.getBuildPlan()!;
            expect(
               Object.values(plan.sources).map((x) => x.name),
            ).not.toContain("rollup");
            return Object.values(plan.refusedSources ?? {}).find(
               (r) => r.name === "rollup",
            );
         };
         const expectRefusal = (
            refusal: Awaited<ReturnType<typeof refusalOf>>,
         ) => {
            expect(refusal?.reason).toBe("upstream_table_unusable");
            // The warehouse table and the source reaching it, never framed
            // as a string comparison the destination cannot reproduce.
            expect(refusal?.message).toContain(
               "it reads the source warehouse (regions) through 'daily_regional'",
            );
            expect(refusal?.message).not.toContain("byte-exact");
         };
         // Unbound: refused at plan.
         expectRefusal(await refusalOf());
         // A standalone run builds `daily` alone and binds it.
         const m = await build();
         expect([m.status, m.error]).toEqual(["MANIFEST_FILE_READY", null]);
         const daily = entryOf(m, "daily");
         expect(entriesOf(m).map((e) => e.sourceName)).not.toContain("rollup");
         // Bound: the same refusal, so the plan a host reads and the plan a
         // run builds from agree on it.
         expectRefusal(await refusalOf());
         // The host's first orchestrated build after that run, the one that
         // reported success with nothing built or failed: every instructed
         // source ends in entries or failures.
         const created = await new MaterializationController(
            service,
         ).createMaterialization(ENV, PKG, {
            buildInstructions: {
               sources: [
                  {
                     sourceEntityId: daily.sourceEntityId,
                     materializedTableId: "host-daily",
                     physicalTableName: "host_daily_t",
                     realization: "COPY",
                     destination: LAKE,
                  },
               ],
            },
         });
         const hosted = await settled(created.id);
         expect([hosted.status, hosted.error]).toEqual([
            "MANIFEST_FILE_READY",
            null,
         ]);
         expect(entryOf(hosted, "daily").physicalTableName).toBe(
            "host_daily_t",
         );
         // `rollup` answers live, equal to live.
         const served = await answer("m.malloy", WAREHOUSE_JOIN_QUERY);
         expect(served.rows).toEqual(live.rows);
      });

      it("refuses at plan a downstream of a source the plan refused, naming it, so a run never fails it", async () => {
         await setUp({ "m.malloy": WAREHOUSE_JOIN_CHAIN_MODEL });
         const live = await answer(
            "m.malloy",
            "run: summary -> { select: total_grand }",
         );
         const pkg = await env.getPackage(parsePackageName(PKG), false);
         const plan = pkg.getBuildPlan()!;
         const planned = Object.values(plan.sources).map((x) => x.name);
         expect(planned).not.toContain("rollup");
         expect(planned).not.toContain("summary");
         const refusals = Object.values(plan.refusedSources ?? {});
         const summary = refusals.find((r) => r.name === "summary");
         expect(summary?.reason).toBe("upstream_table_unusable");
         expect(summary?.message).toContain("its upstream 'rollup'");
         // A run builds what the plan lists, and fails nothing.
         const m = await build();
         expect([m.status, m.error]).toEqual(["MANIFEST_FILE_READY", null]);
         expect(entriesOf(m).map((e) => e.sourceName)).toEqual(["daily"]);
         expect(
            Object.keys(
               (m.manifest as { failures?: Record<string, unknown> })
                  .failures ?? {},
            ),
         ).toEqual([]);
         const served = await answer(
            "m.malloy",
            "run: summary -> { select: total_grand }",
         );
         expect(served.rows).toEqual(live.rows);
      });

      it("builds gtm-funnel's shape (an include over a query stage, joins through a join, a name= on each) from the warehouse, strict, equal to live", async () => {
         await setUp({ "m.malloy": GTM_SHAPE_MODEL });
         const live = await answer("m.malloy", GTM_SHAPE_QUERY);
         const liveJoined = await answer("m.malloy", GTM_SHAPE_JOINED_QUERY);
         expect(live.servedBy).toBe("live");
         const m = await build({ strictUpstreams: true });
         expect([m.status, m.error]).toEqual(["MANIFEST_FILE_READY", null]);
         const base = entryOf(m, "deals_including_deleted");
         const deals = entryOf(m, "deals");
         expect(deals.physicalTableName).not.toBe(base.physicalTableName);
         // The query stage reads no stored source, so neither table is a
         // chained build: each is its own warehouse build, and the stacked
         // path that refused deals2 is never taken.
         expect(
            statements.filter(
               (x) =>
                  /\bCREATE\b/i.test(x) &&
                  x.includes(`"${deals.physicalTableName}"`),
            )[0],
         ).toContain(RAW_MARK);
         expect(asStrings(await storedRows(deals.physicalTableName))).toEqual(
            asStrings(await plannedRows(deals)),
         );
         const served = await answer("m.malloy", GTM_SHAPE_QUERY);
         expect(served.servedBy).toBe("materialized");
         expect(asStrings(served.rows)).toEqual(asStrings(live.rows));
         // A field read through its warehouse joins answers live, equal.
         const joined = await answer("m.malloy", GTM_SHAPE_JOINED_QUERY);
         expect(asStrings(joined.rows)).toEqual(asStrings(liveJoined.rows));
      });

      it("refuses the same shape when neither names its table: never recomputed, answered live", async () => {
         await setUp({ "m.malloy": extendOnlyModel(false) });
         const live = await answer("m.malloy", EXTEND_ONLY_QUERY);
         const m = await build({ strictUpstreams: true });
         expect(
            (entryOf(m, /^(d_incl|deals2)$/) as { error?: string }).error ?? "",
         ).toContain(
            "a downstream is never built by recomputing its upstreams from raw",
         );
         const served = await answer("m.malloy", EXTEND_ONLY_QUERY);
         expect(served.servedBy).toBe("live");
         expect(asStrings(served.rows)).toEqual(asStrings(live.rows));
      });

      it("rebuilds a #(materialize) base's table of unknown age for a windowed storage= rollup, and builds the rollup from it", async () => {
         await setUp({ "m.malloy": materializeRollupModel() });
         const first = await build();
         expect(first.status).toBe("MANIFEST_FILE_READY");
         fs.writeFileSync(
            path.join(envPath, PKG, "m.malloy"),
            materializeRollupModel("sub", ' freshness.window="1h"'),
         );
         await env.getPackage(parsePackageName(PKG), true);
         await reopenServe();
         const second = await build();
         expect([second.status, second.error]).toEqual([
            "MANIFEST_FILE_READY",
            null,
         ]);
         const rollup = entryOf(second, /^orders__rollup__/);
         expect(rollup.upstreamReuse).toBe("reused");
         expect(createdBy(rollup.physicalTableName)).not.toContain(RAW_MARK);
         expect(await storedRows(rollup.physicalTableName)).toEqual(
            await plannedRows(rollup),
         );
      });
   },
);
