// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The R4 harness builds its tables by hand: it runs each plan's build SQL
 * itself and binds the result the way it believes a run does. This spec
 * builds the same small models through the real {@link MaterializationService}
 * and checks that the harness still agrees with it, table for table and
 * answer for answer, for a colocated `#@ persist` table, a `storage=` one, and
 * a colocated `#(preaggregate)` rollup. A difference here means R4 is proving
 * something about a build no run performs.
 *
 * The storage case fakes the DuckLake seams as the other DuckLake component
 * specs do: the attaches open a catalog kept in a local file, the warehouse
 * (a DuckDB connection) is reported to the build as Postgres so the
 * passthrough build is taken, and the serve destination is a DuckDB with the
 * file catalog attached read-only.
 */

import { DuckDBConnection as MalloyDuckDBConnection } from "@malloydata/db-duckdb";
import { MalloyConfig } from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it, spyOn } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { unrecordedAudit } from "../../../src/service/access_record";
import * as connection from "../../../src/service/connection";
import { recordConnectionsAsForTests } from "../../../src/service/connection_type";
import { Environment } from "../../../src/service/environment";
import type { EnvironmentStore } from "../../../src/service/environment_store";
import { parsePackageName } from "../../../src/service/identity";
import { MaterializationService } from "../../../src/service/materialization/run/materialization_service";
import { QueryPrivileges } from "../../../src/service/model_query_request";
import type { Package } from "../../../src/service/package";
import type {
   Materialization,
   ResourceRepository,
} from "../../../src/storage/DatabaseInterface";
import { DuckDBConnection } from "../../../src/storage/duckdb/DuckDBConnection";
import { createDuckDBRepository } from "../../../src/storage/duckdb/DuckDBRepository";
import { initializeSchema } from "../../../src/storage/duckdb/schema";
import { freshLoadPool } from "../../../src/test_helpers/conformance/package_runner";
import { installSettings } from "../../../src/test_helpers/settings_env";
import { writePackageFiles } from "../../../src/test_helpers/temp_package";
import {
   buildScenario,
   DESTINATION,
   disposeScenarios,
   rowDifference,
   type R4Fixture,
   type Row,
} from "./harness";

const ENV = "e1";
const ENV_ID = "env-1";
const PKG = "pkg";
const LAKE = "lake";

const RAW = `source: raw is warehouse.sql("""
  SELECT * FROM (VALUES
    ('a', 'east', 1.0), ('a', 'west', 2.0), ('b', 'east', 4.0), ('c', 'west', 8.0)
  ) AS t(cat, region, amt)
""")`;

/**
 * The colocated model, all in the warehouse: a `#(materialize)` table, a
 * `#@ persist` one, and a rollup.
 */
const COLOCATED = `##! experimental { persistence }
${RAW}

#(materialize) name="by_cat_t"
source: by_cat is raw -> { group_by: cat; aggregate: total is amt.sum() }

#@ persist
source: by_cat_v1 is raw -> { group_by: cat; aggregate: total is amt.sum() }

source: orders is raw extend {
  #(preaggregate) grain=["cat"]
  measure: total is amt.sum()
}
`;

/** The storage model: one table, in the `lake` destination. */
const STORAGE = `##! experimental { persistence }
${RAW}

#@ persist storage="${LAKE}"
source: by_region is raw -> { group_by: region; aggregate: total is amt.sum() }
`;

/** A model's text as the R4 harness reads it: its connection and destination. */
function asR4(model: string): string {
   return model
      .replace(/\bwarehouse\./g, "duckdb.")
      .replace(`storage="${LAKE}"`, `storage="${DESTINATION}"`);
}

const SETTINGS = {
   PERSIST_STORAGE_MODE: "on",
   PUBLISHER_MATERIALIZE: "true",
   PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true",
};

const WAREHOUSE = {
   name: "warehouse",
   type: "duckdb",
   duckdbConnection: { setupSQL: "SELECT 1" },
} as const;

type Entry = { sourceName?: string; physicalTableName: string };

/** `v` as a number when it reads as one, so DECIMAL and DOUBLE compare. */
function asNumber(v: unknown): unknown {
   if (typeof v === "bigint") return Number(v);
   if (typeof v === "string" && v.trim() !== "" && !isNaN(Number(v))) {
      return Number(v);
   }
   return v;
}

/** Rows as sorted tuples of plain values, for comparing two tables' rows. */
function tuples(rows: Row[]): unknown[][] {
   return rows
      .map((r) => Object.values(r).map(asNumber))
      .sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));
}

/** A run through the real service, and its package and repository. */
interface ServiceFixture {
   root: string;
   env: Environment;
   pkg: Package;
   repo: ResourceRepository;
   db: DuckDBConnection;
   service: MaterializationService;
}

async function serviceFixture(
   model: string,
   destinations: unknown[] = [],
   beforeLoad?: (env: Environment) => void,
): Promise<ServiceFixture> {
   const root = fs.realpathSync(
      fs.mkdtempSync(path.join(os.tmpdir(), "r4-harness-drift-")),
   );
   const envPath = path.join(root, "env");
   fs.mkdirSync(envPath);
   writePackageFiles(
      path.join(envPath, PKG),
      { "m.malloy": model },
      { manifest: { name: PKG, description: "fixture" } },
   );
   const db = new DuckDBConnection(":memory:");
   await db.initialize();
   await initializeSchema(db);
   await db.run(
      "INSERT INTO environments (id, name, path, created_at, updated_at) VALUES (?, ?, ?, now(), now())",
      [ENV_ID, ENV, envPath],
   );
   const repo = createDuckDBRepository(db);
   const env = await Environment.create(
      ENV,
      envPath,
      [WAREHOUSE as never],
      destinations as never,
   );
   beforeLoad?.(env);
   await env.addPackage(PKG);
   const pkg = await env.getPackage(parsePackageName(PKG), false);
   const service = new MaterializationService({
      storageManager: { getRepository: () => repo },
      getEnvironment: async () => env,
      getLoadedEnvironments: () => [env],
   } as unknown as EnvironmentStore);
   return { root, env, pkg, repo, db, service };
}

async function disposeServiceFixture(f?: ServiceFixture): Promise<void> {
   if (!f) return;
   await f.env.closeAllConnections().catch(() => undefined);
   await f.db.close();
   fs.rmSync(f.root, { recursive: true, force: true });
}

/** One auto-run, to its settled record. */
async function build(f: ServiceFixture): Promise<Materialization> {
   const created = await f.service.createMaterialization(ENV, PKG, {});
   const running = (
      f.service as unknown as { runningAbortControllers: Map<string, unknown> }
   ).runningAbortControllers;
   const deadline = Date.now() + 60_000;
   for (;;) {
      const m = await f.repo.getMaterializationById(created.id);
      if (
         m &&
         ["MANIFEST_FILE_READY", "FAILED", "CANCELLED"].includes(m.status)
      ) {
         while (running.has(created.id)) {
            await new Promise((resolve) => setTimeout(resolve, 10));
         }
         return (await f.repo.getMaterializationById(created.id)) ?? m;
      }
      if (Date.now() > deadline) throw new Error("run never settled");
      await new Promise((resolve) => setTimeout(resolve, 20));
   }
}

function entries(m: Materialization): Entry[] {
   return Object.values(
      (m.manifest as { entries: Record<string, Entry> }).entries,
   );
}

/** The package's answer to `query`, as rows. */
async function ask(f: ServiceFixture, query: string): Promise<Row[]> {
   const result = await f.pkg
      .getModel("m.malloy")!
      .runQuery(
         unrecordedAudit(),
         { query } as never,
         QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
      );
   return result.compactResult as Row[];
}

/** The answer's path: which table, if any, answered, and through what. */
async function servedBy(
   f: ServiceFixture,
   query: string,
): Promise<{ kind?: string; via?: string }> {
   const result = await f.pkg
      .getModel("m.malloy")!
      .runQuery(
         unrecordedAudit(),
         { query } as never,
         QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
      );
   return {
      kind: result.path?.servedBy?.kind,
      via: result.path?.servedBy?.via,
   };
}

describe("R4 harness drift: colocated tables agree with a real run", () => {
   let restore: () => void;
   let f: ServiceFixture | undefined;
   /** The `#(materialize)` table and the rollup, as R4 builds them. */
   let r4: R4Fixture;
   /** The `#@ persist` table, as R4 builds it. */
   let r4v1: R4Fixture;
   let run: Materialization;

   beforeAll(async () => {
      restore = installSettings({ env: SETTINGS }).restore;
      await freshLoadPool();
      f = await serviceFixture(COLOCATED);
      run = await build(f);
      r4 = await buildScenario({
         files: { "m.malloy": asR4(COLOCATED) },
         seedSql: [],
         materialize: true,
         preaggregate: true,
      });
      r4v1 = await buildScenario({
         files: { "m.malloy": asR4(COLOCATED) },
         seedSql: [],
         colocated: ["by_cat_v1"],
         unbuilt: ["by_cat"],
      });
   }, 120_000);

   afterAll(async () => {
      await disposeServiceFixture(f);
      await disposeScenarios();
      restore();
      await freshLoadPool();
   });

   /** The service's table for a source, by the manifest entry's source name. */
   const serviceTable = (match: (name: string) => boolean) =>
      entries(run).find((e) => match(e.sourceName ?? ""))!.physicalTableName;

   /** A harness's colocated table, by the prefix of its name. */
   async function harnessTable(r: R4Fixture, prefix: string): Promise<string> {
      const found = (
         await r.duckdb.runSQL(
            `SELECT table_name FROM information_schema.tables WHERE table_name LIKE '${prefix}%' ORDER BY table_name`,
         )
      ).rows as { table_name: string }[];
      expect(found.map((t) => t.table_name).length).toBe(1);
      return found[0].table_name;
   }

   async function serviceRows(table: string): Promise<Row[]> {
      const warehouse = await f!.env.getMalloyConnection("warehouse");
      return (await warehouse.runSQL(`SELECT * FROM ${table}`)).rows as Row[];
   }

   async function harnessRows(r: R4Fixture, table: string): Promise<Row[]> {
      return (await r.duckdb.runSQL(`SELECT * FROM ${table}`)).rows as Row[];
   }

   it("the run builds all three tables", () => {
      expect([run.status, run.error]).toEqual(["MANIFEST_FILE_READY", null]);
      expect(entries(run).length).toBe(3);
   });

   it("stores the #(materialize) source's rows as the harness does", async () => {
      expect(
         tuples(
            await serviceRows(
               serviceTable((n) => n.startsWith("by_cat_t") || n === "by_cat"),
            ),
         ),
      ).toEqual(
         tuples(await harnessRows(r4, await harnessTable(r4, "r4_by_cat__"))),
      );
   });

   it("stores the #@ persist source's rows as the harness does", async () => {
      expect(
         tuples(await serviceRows(serviceTable((n) => n === "by_cat_v1"))),
      ).toEqual(
         tuples(
            await harnessRows(r4v1, await harnessTable(r4v1, "r4_by_cat_v1_")),
         ),
      );
   });

   it("stores the rollup's rows as the harness does", async () => {
      const [harnessRollup] = Object.values(r4.rollupTables);
      expect(
         tuples(
            await serviceRows(serviceTable((n) => !n.startsWith("by_cat"))),
         ),
      ).toEqual(tuples(await harnessRows(r4, harnessRollup)));
   });

   it("answers from the rollup as the harness does", async () => {
      const query =
         "run: orders -> { group_by: cat; aggregate: total; order_by: cat }";
      const harness = await r4.run({ name: "rollup", malloy: query });
      expect(harness.servedBy?.kind).toBe("rollup");
      expect((await servedBy(f!, query)).kind).toBe("rollup");
      const served = await ask(f!, query);
      expect(rowDifference(harness.served, served)).toBeUndefined();
      expect(rowDifference(harness.live, served)).toBeUndefined();
   });

   it("answers from the #(materialize) table as the harness does", async () => {
      const query =
         "run: by_cat -> { group_by: cat; aggregate: t is total.sum(); order_by: cat }";
      const harness = await r4.run({ name: "materialize", malloy: query });
      expect(harness.servedVia).toBe("served_copy");
      expect((await servedBy(f!, query)).via).toBe("served_copy");
      const served = await ask(f!, query);
      expect(rowDifference(harness.served, served)).toBeUndefined();
      expect(rowDifference(harness.live, served)).toBeUndefined();
   });

   // Last: it writes a marker row into the `#@ persist` tables, so the
   // marker shows that each side reads its table and not the warehouse.
   it("answers a colocated #@ persist source from its table as the harness does", async () => {
      const query =
         "run: by_cat_v1 -> { group_by: cat; aggregate: t is total.sum(); order_by: cat }";
      const marker = "INSERT INTO %t VALUES ('zz', 99.0)";
      const warehouse = await f!.env.getMalloyConnection("warehouse");
      await warehouse.runSQL(
         marker.replace(
            "%t",
            serviceTable((n) => n === "by_cat_v1"),
         ),
      );
      await r4v1.duckdb.runSQL(
         marker.replace("%t", await harnessTable(r4v1, "r4_by_cat_v1_")),
      );
      const served = await ask(f!, query);
      expect(served.map((r) => r.cat)).toContain("zz");
      const path = await servedBy(f!, query);
      expect(path).toEqual({ kind: "materialized", via: "served_copy" });
      const harness = await r4v1.run({ name: "persist", malloy: query });
      expect(harness.servedVia).toBe("served_copy");
      // A `#@ persist` table, not live: the path's kind for a stored table.
      expect(harness.servedBy?.kind).toBe(path.kind);
      expect(harness.live.map((r) => r.cat)).not.toContain("zz");
      expect(rowDifference(harness.served, served)).toBeUndefined();
   });
});

describe.skipIf(process.platform === "win32")(
   // Skipped on Windows: the catalog file is opened by several in-process
   // DuckDB instances at once, which Windows' mandatory file locks refuse.
   "R4 harness drift: a storage= table agrees with a real run",
   () => {
      let restore: () => void;
      let f: ServiceFixture | undefined;
      let r4: R4Fixture;
      let run: Materialization;
      let lakeDir: string;
      let serve: MalloyDuckDBConnection | undefined;
      let opened = 0;
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

      /** A fresh instance with the catalog attached read-only. */
      async function inspect<T>(
         fn: (c: MalloyDuckDBConnection) => Promise<T>,
      ): Promise<T> {
         const c = new MalloyDuckDBConnection(
            "inspect",
            path.join(lakeDir, `inspect_${++opened}.duckdb`),
         );
         try {
            await attachFileLake(c, LAKE, true);
            try {
               return await fn(c);
            } finally {
               await c.runSQL(`DETACH ${LAKE}`);
            }
         } finally {
            await c.close();
         }
      }

      /** Point the package's serve destination at a fresh read-only attach. */
      async function reopenServe(pkg: Package): Promise<void> {
         await serve?.close().catch(() => undefined);
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
                     path.join(lakeDir, `serve_${++opened}.duckdb`),
                  );
                  await attachFileLake(c, LAKE, true);
                  serve = c;
               }
               return serve;
            },
         }));
         pkg.setServeDestinationConfig(() => config);
      }

      beforeAll(async () => {
         restore = installSettings({ env: SETTINGS }).restore;
         await freshLoadPool();
         lakeDir = fs.realpathSync(
            fs.mkdtempSync(path.join(os.tmpdir(), "r4-harness-drift-lake-")),
         );
         fs.mkdirSync(path.join(lakeDir, "data"), { recursive: true });
         spies.push(
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
            spyOn(
               connection,
               "federateSourceForPassthrough",
            ).mockImplementation(async (session, sourceType) => {
               await session.runSQL(
                  "CREATE OR REPLACE TEMP MACRO postgres_query(alias, q) AS TABLE SELECT * FROM query(q)",
               );
               return { handle: "warehouse", sourceType };
            }),
            // The plan reads the warehouse's recorded type, so it is reported
            // as Postgres there too, or its storage= source is refused.
            {
               mockRestore: recordConnectionsAsForTests({
                  warehouse: "postgres",
               }),
            },
         );
         f = await serviceFixture(
            STORAGE,
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
               },
            ],
            (env) => {
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
            },
         );
         await reopenServe(f.pkg);
         run = await build(f);
         await reopenServe(f.pkg);
         r4 = await buildScenario({
            files: { "m.malloy": asR4(STORAGE) },
            seedSql: [],
         });
      }, 120_000);

      afterAll(async () => {
         for (const spy of spies) spy.mockRestore();
         await serve?.close().catch(() => undefined);
         await disposeServiceFixture(f);
         await disposeScenarios();
         if (lakeDir) fs.rmSync(lakeDir, { recursive: true, force: true });
         restore();
         await freshLoadPool();
      });

      it("the run builds the table", () => {
         expect([run.status, run.error]).toEqual(["MANIFEST_FILE_READY", null]);
         expect(entries(run).map((e) => e.sourceName)).toEqual(["by_region"]);
      });

      it("stores the source's rows as the harness does", async () => {
         const [entry] = entries(run);
         const stored = await inspect(
            async (c) =>
               (
                  await c.runSQL(
                     `SELECT * FROM ${LAKE}.main."${entry.physicalTableName}"`,
                  )
               ).rows as Row[],
         );
         const harness = await r4.storedSQL(
            (t) => `SELECT * FROM ${t("by_region")}`,
         );
         expect(tuples(stored)).toEqual(tuples(harness));
      });

      it("answers from the stored table as the harness does", async () => {
         const query =
            "run: by_region -> { group_by: region; aggregate: t is total.sum(); order_by: region }";
         const harness = await r4.run({ name: "storage", malloy: query });
         expect(harness.servedFrom).toBe("storage");
         const served = await ask(f!, query);
         expect(rowDifference(harness.served, served)).toBeUndefined();
         expect(rowDifference(harness.live, served)).toBeUndefined();
      });
   },
);
